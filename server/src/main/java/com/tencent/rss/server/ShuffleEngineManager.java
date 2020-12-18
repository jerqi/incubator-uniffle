package com.tencent.rss.server;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.tencent.rss.proto.RssProtos.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShuffleEngineManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleEngineManager.class);

  private String appId;
  private String shuffleId;

  private Map<String, ShuffleEngine> engineMap;
  private RangeMap<Integer, String> partitionRangeMap;
  private boolean isCommitted;

  public ShuffleEngineManager(String appId, String shuffleId) {
    this();
    this.appId = appId;
    this.shuffleId = shuffleId;
  }

  public ShuffleEngineManager() {
    engineMap = new ConcurrentHashMap<>();
    partitionRangeMap = TreeRangeMap.create();
    isCommitted = false;
  }

  public StatusCode registerShuffleEngine(
    int startPartition, int endPartition) throws IOException, IllegalStateException {
    ShuffleEngine engine = new ShuffleEngine(appId, shuffleId, startPartition, endPartition);
    return registerShuffleEngine(startPartition, endPartition, engine);
  }

  public StatusCode registerShuffleEngine(
    int startPartition,
    int endPartition,
    ShuffleEngine engine) throws IOException, IllegalStateException {
    String key = ShuffleTaskManager.constructKey(String.valueOf(startPartition), String.valueOf(endPartition));

    ShuffleEngine cur = engineMap.putIfAbsent(key, engine);
    if (cur != null) {
      LOGGER.error("{}~{} {}~{} registered twice.", appId, shuffleId, startPartition, endPartition);
      return StatusCode.DOUBLE_REGISTER;
    }

    synchronized (this) {
      partitionRangeMap.put(Range.closed(startPartition, endPartition), key);
    }

    ShuffleEngine shuffleEngine = engineMap.get(key);
    StatusCode ret = shuffleEngine.init();
    if (ret != StatusCode.SUCCESS) {
      engineMap.remove(key);
      synchronized (this) {
        partitionRangeMap.remove(Range.closed(startPartition, endPartition));
      }
    }

    ShuffleServerMetrics.incRegisteredShuffleEngine();
    return ret;
  }

  public ShuffleEngine getShuffleEngine(int partition) {
    String key = partitionRangeMap.get(partition);
    if (key == null) {
      LOGGER.error("{}~{} Can't find shuffle engine of partition {} from range map", appId, shuffleId, partition);
      return null;
    }

    ShuffleEngine shuffleEngine = engineMap.get(key);
    if (shuffleEngine == null) {
      LOGGER.error("{}~{} Can't find shuffle engine of partition {}from engine map", appId, shuffleId, partition);
    }

    return shuffleEngine;
  }

  public StatusCode commit() throws IOException, IllegalStateException {
    synchronized (this) {
      if (isCommitted) {
        return StatusCode.SUCCESS;
      }

      for (ShuffleEngine engine : engineMap.values()) {
        engine.flush();
      }

      isCommitted = true;
      ShuffleServerMetrics.decRegisteredShuffleEngine();
      return StatusCode.SUCCESS;
    }
  }

}
