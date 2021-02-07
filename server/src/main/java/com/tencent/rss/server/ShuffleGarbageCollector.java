package com.tencent.rss.server;

import static java.util.Objects.requireNonNull;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ShuffleGarbageCollector {

  private final ShuffleServer shuffleServer;
  private final long gcInitialDelay;
  private final long gcInterval;
  private final long gcThreshold;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService;

  public ShuffleGarbageCollector(ShuffleServer shuffleServer) {
    requireNonNull(shuffleServer);
    this.shuffleServer = shuffleServer;
    ShuffleServerConf conf = shuffleServer.getShuffleServerConf();
    this.gcInitialDelay = conf.getLong(ShuffleServerConf.GC_DELAY);
    this.gcInterval = conf.getLong(ShuffleServerConf.GC_INTERVAL);
    this.gcThreshold = conf.getLong(ShuffleServerConf.GC_THRESHOLD);

    int threadNum = conf.getInteger(ShuffleServerConf.GC_THREAD_NUM);
    executorService = Executors.newFixedThreadPool(
        threadNum, NamedDaemonThreadFactory.defaultThreadFactory(true));
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        NamedDaemonThreadFactory.defaultThreadFactory(true));
  }

  public void startGC() {
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        doGC();
      }
    }, gcInitialDelay, gcInterval, TimeUnit.SECONDS);
  }

  public void doGC() {
    List<ShuffleEngineManager> shuffleEngineManagerList =
        new LinkedList<>(shuffleServer.getShuffleTaskManager().getShuffleTaskEngines().values());
    long currentTimestamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    for (ShuffleEngineManager shuffleEngineManager : shuffleEngineManagerList) {
      Runnable runnable = new Runnable() {
        @Override
        public void run() {
          doGC(shuffleEngineManager, currentTimestamp);
        }
      };
      // LOG
      executorService.execute(runnable);
    }
  }

  public void doGC(ShuffleEngineManager shuffleEngineManager, long currentTimestamp) {
    List<String> keys = new LinkedList<>();
    List<Map.Entry<String, ShuffleEngine>> entries = new LinkedList<>(shuffleEngineManager.getEngineMap().entrySet());

    for (Map.Entry<String, ShuffleEngine> entry : entries) {
      long timeStamp = TimeUnit.MILLISECONDS.toSeconds(entry.getValue().getTimestamp());
      long diff = currentTimestamp - timeStamp;
      if (diff >= this.gcThreshold) {
        keys.add(entry.getKey());
      }
    }

    int cnt = 0;
    List<String> engineKeys = new LinkedList<>();
    for (String key : keys) {
      ShuffleEngine shuffleEngine = shuffleEngineManager.getEngineMap().remove(key);
      shuffleEngine.reclaim();
      ++cnt;
      engineKeys.add(shuffleEngine.makeKey());
      shuffleEngine = null;
    }
    ShuffleServerMetrics.gaugeRegisteredShuffleEngine.dec(cnt);

    shuffleServer.getBufferManager().reclaim(engineKeys);

    if (shuffleEngineManager.getEngineMap().isEmpty()) {
      shuffleEngineManager.reclaim();
      String key = ShuffleTaskManager.constructKey(
          shuffleEngineManager.getAppId(), String.valueOf(shuffleEngineManager.getShuffleId()));
      shuffleServer.getShuffleTaskManager().getShuffleTaskEngines().remove(key);
      ShuffleServerMetrics.gaugeRegisteredShuffle.dec();
    }

  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  static class NamedDaemonThreadFactory implements ThreadFactory {

    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private final int poolNumber;
    private final AtomicInteger threadNumber;
    private final ThreadFactory delegate;
    private final boolean daemon;

    NamedDaemonThreadFactory(ThreadFactory delegate, boolean daemon) {
      this.poolNumber = POOL_NUMBER.getAndIncrement();
      this.threadNumber = new AtomicInteger(1);
      this.delegate = delegate;
      this.daemon = daemon;
    }

    static ThreadFactory defaultThreadFactory(boolean daemon) {
      return new NamedDaemonThreadFactory(Executors.defaultThreadFactory(), daemon);
    }

    public Thread newThread(Runnable r) {
      Thread t = this.delegate.newThread(r);
      t.setName(String.format("gc-pool-%d-%d", this.poolNumber, this.threadNumber.getAndIncrement()));
      t.setDaemon(this.daemon);
      return t;
    }
  }


}
