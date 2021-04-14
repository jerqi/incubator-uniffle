package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleDeleteHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleFlushManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleFlushManager.class);
  public static AtomicLong ATOMIC_EVENT_ID = new AtomicLong(0);
  private final ShuffleServer shuffleServer;
  private final BlockingQueue<ShuffleDataFlushEvent> flushQueue = Queues.newLinkedBlockingQueue();
  private final ThreadPoolExecutor threadPoolExecutor;
  private final String[] storageBasePaths;
  private final String shuffleServerId;
  private final String storageType;
  private final int storageDataReplica;
  private final Configuration hadoopConf;
  // appId -> shuffleId -> partitionId -> handlers
  private Map<String, Map<Integer, RangeMap<Integer, ShuffleWriteHandler>>> handlers = Maps.newConcurrentMap();
  // appId -> shuffleId -> committed shuffle block data count
  private Map<String, Map<Long, Integer>> committedBlockCount = Maps.newConcurrentMap();
  private boolean isRunning;
  private Runnable processEventThread;
  private int retryMax;

  public ShuffleFlushManager(ShuffleServerConf shuffleServerConf, String shuffleServerId, ShuffleServer shuffleServer) {
    this.shuffleServerId = shuffleServerId;
    this.shuffleServer = shuffleServer;
    this.hadoopConf = new Configuration();
    retryMax = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_WRITE_RETRY_MAX);
    long keepAliveTime = shuffleServerConf.getLong(ShuffleServerConf.SERVER_FLUSH_THREAD_ALIVE);
    storageType = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE);
    storageDataReplica = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_DATA_REPLICA);
    int waitQueueSize = shuffleServerConf.getInteger(
        ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(waitQueueSize);
    int poolSize = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_SIZE);
    threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize, keepAliveTime, TimeUnit.SECONDS, waitQueue);
    storageBasePaths = shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH).split(",");
    isRunning = true;

    // the thread for flush data
    processEventThread = () -> {
      try {
        while (isRunning) {
          ShuffleDataFlushEvent event = flushQueue.take();
          threadPoolExecutor.execute(() -> {
            flushToFile(event);
          });
        }
      } catch (InterruptedException ie) {
        LOG.error("Exception happened when process event.", ie);
        isRunning = false;
      }
    };
    new Thread(processEventThread).start();
  }

  public void addToFlushQueue(ShuffleDataFlushEvent event) {
    flushQueue.offer(event);
  }

  private void flushToFile(ShuffleDataFlushEvent event) {
    long start = System.currentTimeMillis();
    List<ShufflePartitionedBlock> blocks = event.getShuffleBlocks();
    boolean writeSuccess = false;
    try {
      if (blocks == null || blocks.isEmpty()) {
        LOG.info("There is no block to be flushed: " + event);
      } else {
        ShuffleWriteHandler handler = getHandler(event);
        int retry = 0;
        while (!writeSuccess) {
          if (retry > retryMax) {
            LOG.error("Failed to write data for " + event + " in " + retryMax + " times, shuffle data will be lost");
            break;
          }
          try {
            handler.write(blocks);
            updateCommittedBlockCount(event.getAppId(), event.getShuffleId(), event.getShuffleBlocks().size());
            writeSuccess = true;
            ShuffleServerMetrics.counterTotalWriteDataSize.inc(event.getSize());
            ShuffleServerMetrics.counterTotalWriteBlockSize.inc(event.getShuffleBlocks().size());
          } catch (Exception e) {
            LOG.warn("Exception happened when write data for " + event + ", try again", e);
            Thread.sleep(1000);
          }
          retry++;
        }
      }
    } catch (Exception e) {
      // just log the error, don't throw the exception and stop the flush thread
      LOG.error("Exception happened when process flush shuffle data for " + event, e);
    } finally {
      if (shuffleServer != null) {
        shuffleServer.getShuffleBufferManager().releaseMemory(event.getSize(), true);
        long duration = System.currentTimeMillis() - start;
        if (writeSuccess) {
          LOG.debug("Flush to file success in " + duration + " ms and release " + event.getSize() + " bytes");
        } else {
          LOG.error("Flush to file for " + event + " failed in "
              + duration + " ms and release " + event.getSize() + " bytes");
        }
      }
    }
  }

  private synchronized ShuffleWriteHandler getHandler(ShuffleDataFlushEvent event) throws Exception {
    handlers.putIfAbsent(event.getAppId(), Maps.newConcurrentMap());
    Map<Integer, RangeMap<Integer, ShuffleWriteHandler>> shuffleIdToHandlers = handlers.get(event.getAppId());
    shuffleIdToHandlers.putIfAbsent(event.getShuffleId(), TreeRangeMap.create());
    RangeMap<Integer, ShuffleWriteHandler> eventIdRangeMap = shuffleIdToHandlers.get(event.getShuffleId());
    if (eventIdRangeMap.get(event.getStartPartition()) == null) {
      eventIdRangeMap.put(Range.closed(event.getStartPartition(), event.getEndPartition()),
          ShuffleHandlerFactory.getInstance().createShuffleWriteHandler(
              new CreateShuffleWriteHandlerRequest(
                  storageType, event.getAppId(), event.getShuffleId(), event.getStartPartition(),
                  event.getEndPartition(), storageBasePaths, shuffleServerId, hadoopConf, storageDataReplica)));
    }
    return eventIdRangeMap.get(event.getStartPartition());
  }

  private void updateCommittedBlockCount(String appId, long shuffleId, int blockNum) {
    committedBlockCount.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Long, Integer> committedBlocks = committedBlockCount.get(appId);
    committedBlocks.putIfAbsent(shuffleId, 0);
    synchronized (committedBlocks) {
      committedBlocks.put(shuffleId, committedBlocks.get(shuffleId) + blockNum);
    }
  }

  public int getCommittedBlockCount(String appId, long shuffleId) {
    if (!committedBlockCount.containsKey(appId)) {
      return 0;
    }
    Map<Long, Integer> committedBlocks = committedBlockCount.get(appId);
    if (!committedBlocks.containsKey(shuffleId)) {
      return 0;
    }
    return committedBlocks.get(shuffleId);
  }

  public void removeResources(String appId) {
    handlers.remove(appId);
    committedBlockCount.remove(appId);
    // delete shuffle data for application
    ShuffleDeleteHandler deleteHandler = ShuffleHandlerFactory.getInstance()
        .createShuffleDeleteHandler(new CreateShuffleDeleteHandlerRequest(storageType, hadoopConf));
    deleteHandler.delete(storageBasePaths, appId);
  }

  public int getEventNumInFlush() {
    return flushQueue.size();
  }

  @VisibleForTesting
  protected Map<String, Map<Integer, RangeMap<Integer, ShuffleWriteHandler>>> getHandlers() {
    return handlers;
  }
}
