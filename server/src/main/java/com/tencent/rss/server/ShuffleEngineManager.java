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

import static java.util.Objects.requireNonNull;

public class ShuffleEngineManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleEngineManager.class);

  private final String appId;
  private final String shuffleId;
  private final ShuffleServerConf conf;
  private final BufferManager bufferManager;
  private final String serverId;

  private Map<String, ShuffleEngine> engineMap;
  private RangeMap<Integer, String> partitionRangeMap;

  public ShuffleEngineManager(
    String appId, String shuffleId, ShuffleServerConf conf, BufferManager bufferManager, String serverId) {
    engineMap = new ConcurrentHashMap<>();
    partitionRangeMap = TreeRangeMap.create();
    this.appId = appId;
    this.shuffleId = shuffleId;
    requireNonNull(conf);
    requireNonNull(bufferManager);
    this.conf = conf;
    this.bufferManager = bufferManager;
    this.serverId = serverId;
  }

  public ShuffleEngineManager(String appId, String shuffleId) {
    engineMap = new ConcurrentHashMap<>();
    partitionRangeMap = TreeRangeMap.create();
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.conf = null;
    this.bufferManager = null;
    this.serverId = "";
  }

  public StatusCode registerShuffleEngine(int startPartition, int endPartition) {
    ShuffleEngine engine =
      new ShuffleEngine(appId, shuffleId, startPartition, endPartition, conf, bufferManager, serverId);
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
      LOGGER.error("{}~{} {}~{} registered twice.", appId, shuffleId, startPartition, endPartition);
      return StatusCode.DOUBLE_REGISTER;
    }

    synchronized (this) {
      partitionRangeMap.put(Range.closed(startPartition, endPartition), key);
    }
    ShuffleServerMetrics.incRegisteredShuffleEngine();

    return ret;
  }

  public Map<String, ShuffleEngine> getEngineMap() {
    return this.engineMap;
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

  public synchronized StatusCode commit() throws IOException, IllegalStateException {
    for (ShuffleEngine engine : engineMap.values()) {
      engine.commit();
    }

    ShuffleServerMetrics.decRegisteredShuffleEngine();
    return StatusCode.SUCCESS;
  }

  public synchronized void reclaim() {
    partitionRangeMap.clear();
  }

  public String getAppId() {
    return this.appId;
  }

  public String getShuffleId() {
    return this.shuffleId;
  }

}
