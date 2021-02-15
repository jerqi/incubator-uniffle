package com.tencent.rss.server;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleEngineManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleEngineManager.class);

  private final String appId;
  private final int shuffleId;
  private final ShuffleServerConf conf;
  private final BufferManager bufferManager;
  private final ShuffleFlushManager shuffleFlushManager;

  private Map<String, ShuffleEngine> engineMap;
  private RangeMap<Integer, String> partitionRangeMap;

  public ShuffleEngineManager(
      String appId,
      int shuffleId,
      ShuffleServerConf conf,
      BufferManager bufferManager,
      ShuffleFlushManager shuffleFlushManager) {
    requireNonNull(conf);
    requireNonNull(bufferManager);
    engineMap = Maps.newConcurrentMap();
    partitionRangeMap = TreeRangeMap.create();
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.conf = conf;
    this.bufferManager = bufferManager;
    this.shuffleFlushManager = shuffleFlushManager;
  }

  public ShuffleEngineManager(String appId, int shuffleId) {
    engineMap = new ConcurrentHashMap<>();
    partitionRangeMap = TreeRangeMap.create();
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.conf = null;
    this.bufferManager = null;
    this.shuffleFlushManager = null;
  }

  public StatusCode registerShuffleEngine(int startPartition, int endPartition) {
    ShuffleEngine engine =
        new ShuffleEngine(appId, shuffleId, startPartition, endPartition,
            conf, bufferManager);
    return registerShuffleEngine(startPartition, endPartition, engine);
  }

  public StatusCode registerShuffleEngine(int startPartition, int endPartition, ShuffleEngine engine) {
    StatusCode ret = engine.init();
    if (ret != StatusCode.SUCCESS) {
      return ret;
    }

    String key = ShuffleTaskManager.constructKey(String.valueOf(startPartition), String.valueOf(endPartition));
    ShuffleEngine cur = engineMap.putIfAbsent(key, engine);
    if (cur != null) {
      LOG.error("{}~{} {}~{} registered twice.", appId, shuffleId, startPartition, endPartition);
      return StatusCode.DOUBLE_REGISTER;
    }

    synchronized (this) {
      partitionRangeMap.put(Range.closed(startPartition, endPartition), key);
    }
    ShuffleServerMetrics.gaugeRegisteredShuffleEngine.inc(1.0);

    return ret;
  }

  public Map<String, ShuffleEngine> getEngineMap() {
    return this.engineMap;
  }

  public ShuffleEngine getShuffleEngine(int partition) {
    String key = partitionRangeMap.get(partition);
    if (key == null) {
      LOG.error("{}~{} Can't find shuffle engine of partition {} from range map", appId, shuffleId, partition);
      return null;
    }

    ShuffleEngine shuffleEngine = engineMap.get(key);
    if (shuffleEngine == null) {
      LOG.error("{}~{} Can't find shuffle engine of partition {}from engine map", appId, shuffleId, partition);
    }

    return shuffleEngine;
  }

  public synchronized StatusCode commit() throws Exception {
    Map<String, Set<Long>> pathToEventIds = Maps.newHashMap();
    for (ShuffleEngine engine : engineMap.values()) {
      Set<Long> eventIds = engine.commit();

      // skip empty eventId
      if (!eventIds.isEmpty()) {
        String path = ShuffleStorageUtils.getShuffleDataPath(
            appId, shuffleId, engine.getStartPartition(), engine.getEndPartition());
        pathToEventIds.put(path, eventIds);
        LOG.info("Commit for " + path + " and get expectedEventIds: " + eventIds);
      }
    }

    long commitTimeout = conf.get(ShuffleServerConf.SERVER_COMMIT_TIMEOUT);
    long start = System.currentTimeMillis();
    while (true) {
      List<String> removedKeys = Lists.newArrayList();
      for (Map.Entry<String, Set<Long>> entry : pathToEventIds.entrySet()) {
        String path = entry.getKey();
        Set<Long> committedIds = shuffleFlushManager.getEventIds(path);
        if (committedIds != null && !committedIds.isEmpty()) {
          Set<Long> expectedEventIds = entry.getValue();
          LOG.debug("Got expectedEventIds for " + path + ": "
              + expectedEventIds + ", current commit: " + committedIds);
          expectedEventIds.removeAll(committedIds);
          if (expectedEventIds.isEmpty()) {
            removedKeys.add(path);
          }
        }
      }
      for (String key : removedKeys) {
        pathToEventIds.remove(key);
        LOG.info("Path " + key + " is committed for current request.");
      }
      if (pathToEventIds.isEmpty()) {
        break;
      }
      Thread.sleep(1000);
      if (System.currentTimeMillis() - start > commitTimeout) {
        throw new RuntimeException("Shuffle data commit timeout for " + commitTimeout + " ms");
      }
      LOG.info("Checking commit result for appId[" + appId + "], shuffleId[" + shuffleId + "]");
    }

    return StatusCode.SUCCESS;
  }

  public synchronized void reclaim() {
    partitionRangeMap.clear();
  }

  public String getAppId() {
    return this.appId;
  }

  public int getShuffleId() {
    return this.shuffleId;
  }
}
