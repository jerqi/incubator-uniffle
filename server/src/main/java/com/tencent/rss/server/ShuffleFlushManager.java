package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeRangeMap;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final Configuration hadoopConf;
  // appId -> shuffleId -> partitionId -> eventIds
  private Map<String, Map<Integer, RangeMap<Integer, Set<Long>>>> eventIds = Maps.newConcurrentMap();
  // appId -> shuffleId -> partitionId -> handlers
  private Map<String, Map<Integer, RangeMap<Integer, ShuffleWriteHandler>>> handlers = Maps.newConcurrentMap();
  private boolean isRunning;
  private Runnable processEventThread;

  public ShuffleFlushManager(ShuffleServerConf shuffleServerConf, String shuffleServerId, ShuffleServer shuffleServer) {
    this.shuffleServerId = shuffleServerId;
    this.shuffleServer = shuffleServer;
    this.hadoopConf = new Configuration();
    hadoopConf.setInt("dfs.replication", shuffleServerConf.getInteger(ShuffleServerConf.DATA_STORAGE_REPLICA));
    int poolSize = shuffleServerConf.getInteger(ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_SIZE);
    long keepAliveTime = shuffleServerConf.getLong(ShuffleServerConf.SERVER_FLUSH_THREAD_ALIVE);
    storageType = shuffleServerConf.get(RssBaseConf.DATA_STORAGE_TYPE);
    int waitQueueSize = shuffleServerConf.getInteger(
        ShuffleServerConf.SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(waitQueueSize);
    threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize, keepAliveTime, TimeUnit.SECONDS, waitQueue);
    storageBasePaths = shuffleServerConf.getString(ShuffleServerConf.DATA_STORAGE_BASE_PATH).split(",");
    isRunning = true;

    // the thread for flush data
    processEventThread = () -> {
      try {
        while (isRunning) {
          ShuffleDataFlushEvent event = flushQueue.take();
          threadPoolExecutor.execute(() -> flushToFile(event));
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
    long startTime = System.currentTimeMillis();
    List<ShufflePartitionedBlock> blocks = event.getShuffleBlocks();

    try {
      if (blocks == null || blocks.isEmpty()) {
        LOG.info("There is no block to be flushed: " + event);
      } else {
        ShuffleWriteHandler handler = getHandler(event);
        handler.write(blocks);

        long writeSize = event.getSize();
        long writeTime = System.currentTimeMillis() - startTime;
        ShuffleServerMetrics.counterTotalWriteDataSize.inc(writeSize);

        if (writeTime != 0) {
          double writeSpeed = ((double) writeSize) / ((double) writeTime) / 1000.0;
          ShuffleServerMetrics.histogramWriteSpeed.observe(writeSpeed);
        }
      }

      addEventId(event);
    } catch (Exception e) {
      // just log the error, don't throw the exception and stop the flush thread
      LOG.error("Exception happened when process flush shuffle data for " + event, e);
    } finally {
      if (shuffleServer != null) {
        shuffleServer.getShuffleBufferManager().releaseMemory(event.getSize());
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
                  event.getEndPartition(), storageBasePaths, shuffleServerId, hadoopConf)));
    }
    return eventIdRangeMap.get(event.getStartPartition());
  }

  private synchronized void addEventId(ShuffleDataFlushEvent event) {
    eventIds.putIfAbsent(event.getAppId(), Maps.newConcurrentMap());
    Map<Integer, RangeMap<Integer, Set<Long>>> shuffleIdToEventIds = eventIds.get(event.getAppId());
    shuffleIdToEventIds.putIfAbsent(event.getShuffleId(), TreeRangeMap.create());
    RangeMap<Integer, Set<Long>> eventIdRangeMap = shuffleIdToEventIds.get(event.getShuffleId());
    if (eventIdRangeMap.get(event.getStartPartition()) == null) {
      eventIdRangeMap.put(Range.closed(event.getStartPartition(), event.getEndPartition()), Sets.newHashSet());
    }
    eventIdRangeMap.get(event.getStartPartition()).add(event.getEventId());
  }

  public Set<Long> getEventIds(String appId, int shuffleId, Range<Integer> range) {
    Map<Integer, RangeMap<Integer, Set<Long>>> shuffleIdToEventIds = eventIds.get(appId);
    if (shuffleIdToEventIds == null) {
      return Sets.newHashSet();
    }
    RangeMap<Integer, Set<Long>> eventIdRangeMap = shuffleIdToEventIds.get(shuffleId);
    if (eventIdRangeMap == null) {
      return Sets.newHashSet();
    }
    return eventIdRangeMap.get(range.lowerEndpoint());
  }

  @VisibleForTesting
  protected Map<String, Map<Integer, RangeMap<Integer, Set<Long>>>> getEventIds() {
    return eventIds;
  }

  public void removeResources(String appId) {
    eventIds.remove(appId);
    handlers.remove(appId);
  }

  @VisibleForTesting
  protected Map<String, Map<Integer, RangeMap<Integer, ShuffleWriteHandler>>> getHandlers() {
    return handlers;
  }
}
