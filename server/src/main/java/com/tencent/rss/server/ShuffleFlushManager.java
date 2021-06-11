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
  private final ShuffleServerConf shuffleServerConf;
  private Configuration hadoopConf;
  // appId -> shuffleId -> partitionId -> handlers
  private Map<String, Map<Integer, RangeMap<Integer, ShuffleWriteHandler>>> handlers = Maps.newConcurrentMap();
  // appId -> shuffleId -> committed shuffle block data count
  private Map<String, Map<Long, Integer>> committedBlockCount = Maps.newConcurrentMap();
  private boolean isRunning;
  private Runnable processEventThread;
  private int retryMax;
  private long writeSlowThreshold;
  private long eventSizeThresholdL1;
  private long eventSizeThresholdL2;
  private long eventSizeThresholdL3;

  public ShuffleFlushManager(ShuffleServerConf shuffleServerConf, String shuffleServerId, ShuffleServer shuffleServer) {
    this.shuffleServerId = shuffleServerId;
    this.shuffleServer = shuffleServer;
    this.shuffleServerConf = shuffleServerConf;
    initHadoopConf();
    retryMax = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_WRITE_RETRY_MAX);
    storageType = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE);
    storageDataReplica = shuffleServerConf.get(RssBaseConf.RSS_STORAGE_DATA_REPLICA);
    writeSlowThreshold = shuffleServerConf.getLong(ShuffleServerConf.SERVER_WRITE_SLOW_THRESHOLD);
    eventSizeThresholdL1 = shuffleServerConf.getLong(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L1);
    eventSizeThresholdL2 = shuffleServerConf.getLong(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L2);
    eventSizeThresholdL3 = shuffleServerConf.getLong(ShuffleServerConf.SERVER_EVENT_SIZE_THRESHOLD_L3);
    int waitQueueSize = shuffleServerConf.getInteger(
        ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(waitQueueSize);
    int poolSize = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_SIZE);
    long keepAliveTime = shuffleServerConf.getLong(ShuffleServerConf.SERVER_FLUSH_THREAD_ALIVE);
    threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize, keepAliveTime, TimeUnit.SECONDS, waitQueue);
    storageBasePaths = shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH).split(",");
    isRunning = true;

    // the thread for flush data
    processEventThread = () -> {
      try {
        while (isRunning) {
          ShuffleDataFlushEvent event = flushQueue.take();
          threadPoolExecutor.execute(() -> {
            ShuffleServerMetrics.gaugeEventQueueSize.set(flushQueue.size());
            ShuffleServerMetrics.gaugeWriteHandler.inc();
            flushToFile(event);
            ShuffleServerMetrics.gaugeWriteHandler.dec();
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
            long startWrite = System.currentTimeMillis();
            handler.write(blocks);
            long writeTime = System.currentTimeMillis() - startWrite;
            ShuffleServerMetrics.counterTotalWriteTime.inc(writeTime);
            ShuffleServerMetrics.counterWriteTotal.inc();
            if (writeTime > writeSlowThreshold) {
              ShuffleServerMetrics.counterWriteSlow.inc();
            }
            updateCommittedBlockCount(event.getAppId(), event.getShuffleId(), event.getShuffleBlocks().size());
            writeSuccess = true;
            ShuffleServerMetrics.counterTotalWriteDataSize.inc(event.getSize());
            ShuffleServerMetrics.counterTotalWriteBlockSize.inc(event.getShuffleBlocks().size());
            if (event.getSize() < eventSizeThresholdL1) {
              ShuffleServerMetrics.counterEventSizeThresholdLevel1.inc();
            } else if (event.getSize() < eventSizeThresholdL2) {
              ShuffleServerMetrics.counterEventSizeThresholdLevel2.inc();
            } else if (event.getSize() < eventSizeThresholdL3) {
              ShuffleServerMetrics.counterEventSizeThresholdLevel3.inc();
            } else {
              ShuffleServerMetrics.counterEventSizeThresholdLevel4.inc();
            }
          } catch (Exception e) {
            LOG.warn("Exception happened when write data for " + event + ", try again", e);
            ShuffleServerMetrics.counterWriteException.inc();
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
        shuffleServer.getShuffleBufferManager().releaseMemory(event.getSize(), true, false);
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

  protected void initHadoopConf() {
    hadoopConf = new Configuration();
    for (String key : shuffleServerConf.getKeySet()) {
      if (key.startsWith(ShuffleServerConf.PREFIX_HADOOP_CONF)) {
        String value = shuffleServerConf.getString(key, "");
        String hadoopKey = key.substring(ShuffleServerConf.PREFIX_HADOOP_CONF.length() + 1);
        LOG.info("Update hadoop configuration:" + hadoopKey + "=" + value);
        hadoopConf.set(hadoopKey, value);
      }
    }
  }

  public int getEventNumInFlush() {
    return flushQueue.size();
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @VisibleForTesting
  protected Map<String, Map<Integer, RangeMap<Integer, ShuffleWriteHandler>>> getHandlers() {
    return handlers;
  }
}
