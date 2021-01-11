package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.server.ShuffleGarbageCollector.NamedDaemonThreadFactory;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleFlushManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleFlushManager.class);
  private static final Logger LOG_RSS_INFO = LoggerFactory.getLogger(Constants.LOG4J_RSS_SHUFFLE_PREFIX);
  public static AtomicLong ATOMIC_EVENT_ID = new AtomicLong(0);
  private ScheduledExecutorService scheduledExecutorService;
  private BlockingQueue<ShuffleDataFlushEvent> flushQueue = Queues.newLinkedBlockingQueue();
  private ConcurrentMap<String, FileBasedShuffleWriteHandler> pathToHandler = Maps.newConcurrentMap();
  private ConcurrentMap<String, Set<Long>> pathToEventIds = Maps.newConcurrentMap();
  private ThreadPoolExecutor threadPoolExecutor;
  private ShuffleServerConf shuffleServerConf;
  private final ShuffleServer shuffleServer;
  private boolean isRunning;
  private String storageBasePath;
  private String shuffleServerId;
  private long expired;
  private Runnable processEventThread;

  public ShuffleFlushManager(ShuffleServerConf shuffleServerConf, String shuffleServerId, ShuffleServer shuffleServer) {
    this.shuffleServerId = shuffleServerId;
    this.shuffleServerConf = shuffleServerConf;
    this.shuffleServer = shuffleServer;
    int poolSize = shuffleServerConf.getInteger(ShuffleServerConf.RSS_SHUFFLE_SERVER_FLUSH_THREAD_POOL_SIZE);
    long keepAliveTime = shuffleServerConf.getLong(ShuffleServerConf.RSS_SHUFFLE_SERVER_FLUSH_THREAD_ALIVE);
    int waitQueueSize = shuffleServerConf.getInteger(
        ShuffleServerConf.RSS_SHUFFLE_SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE);
    BlockingQueue<Runnable> waitQueue = Queues.newLinkedBlockingQueue(waitQueueSize);
    threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize, keepAliveTime, TimeUnit.SECONDS, waitQueue);
    storageBasePath = shuffleServerConf.getString(ShuffleServerConf.DATA_STORAGE_BASE_PATH);
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

    expired = shuffleServerConf.getLong(
        ShuffleServerConf.RSS_SHUFFLE_SERVER_FLUSH_HANDLER_EXPIRED);
    long checkInterval = shuffleServerConf.getLong(
        ShuffleServerConf.RSS_SHUFFLE_SERVER_FLUSH_GC_CHECK_INTERVAL);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        NamedDaemonThreadFactory.defaultThreadFactory(true));
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        clearHandler();
      }
    }, checkInterval, checkInterval, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  protected ConcurrentMap<String, FileBasedShuffleWriteHandler> getPathToHandler() {
    return pathToHandler;
  }

  public void clearHandler() {
    List<String> removedKeys = Lists.newArrayList();
    try {
      for (Map.Entry<String, FileBasedShuffleWriteHandler> entry : pathToHandler.entrySet()) {
        FileBasedShuffleWriteHandler handler = entry.getValue();
        if (handler != null) {
          long duration = (System.currentTimeMillis() - handler.getAccessTime()) / 1000;
          LOG_RSS_INFO.info("Handler for " + entry.getKey() + ", duration=" + duration);
          if (duration > expired) {
            removedKeys.add(entry.getKey());
          }
        }
      }
      synchronized (this) {
        for (String key : removedKeys) {
          pathToHandler.remove(key);
          pathToEventIds.remove(key);
        }
      }
      LOG_RSS_INFO.info("Successfully remove handlers/eventIds for " + removedKeys);
    } catch (Exception e) {
      // ignore exception in gc process
      LOG_RSS_INFO.warn("Failed remove handlers for " + removedKeys, e);
    }
  }

  public void addToFlushQueue(ShuffleDataFlushEvent event) {
    flushQueue.offer(event);
  }

  private void flushToFile(ShuffleDataFlushEvent event) {
    String path = event.getShuffleFilePath();
    String shuffleDataFolder = RssUtils.getFullShuffleDataFolder(storageBasePath, path);
    List<ShufflePartitionedBlock> blocks = event.getShuffleBlocks();
    try {
      if (blocks == null || blocks.isEmpty()) {
        LOG_RSS_INFO.info("There is no block to be flushed: " + event);
      } else {
        FileBasedShuffleWriteHandler handler;
        synchronized (this) {
          pathToHandler.putIfAbsent(path,
              new FileBasedShuffleWriteHandler(shuffleDataFolder, shuffleServerId, new Configuration()));
          handler = pathToHandler.get(path);
        }
        handler.write(blocks);
        LOG_RSS_INFO.info("Write data success for " + event.toString());
      }
      pathToEventIds.putIfAbsent(path, Sets.newConcurrentHashSet());
      pathToEventIds.get(path).add(event.getEventId());
    } catch (Exception e) {
      // just log the error, don't throw the exception and stop the flush thread
      LOG.error("Exception happened when process flush shuffle data for folder["
          + shuffleDataFolder + "], blocks[" + blocks.toString() + "]", e);
    } finally {
      if (shuffleServer != null) {
        shuffleServer.getBufferManager().updateSize(-event.getSize());
      }
    }
  }

  public Set<Long> getEventIds(String path) {
    return pathToEventIds.get(path);
  }
}
