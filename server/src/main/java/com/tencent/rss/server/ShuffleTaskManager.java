package com.tencent.rss.server;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleTaskManager {

  public static final String KEY_DELIMITER = "~";
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleEngine.class);
  private final BufferManager bufferManager;
  private final ShuffleFlushManager shuffleFlushManager;
  private final String serverId;
  private ShuffleServerConf conf;
  private Map<String, ShuffleEngineManager> shuffleTaskEngines;
  // appId -> shuffleId -> partitionId -> blockIds to avoid too many appId
  private Map<String, Map<Integer, Map<Integer, List<Long>>>> partitionsToBlockIds;

  public ShuffleTaskManager() {
    this.bufferManager = null;
    this.shuffleFlushManager = null;
    this.conf = null;
    this.serverId = "";
    this.shuffleTaskEngines = Maps.newConcurrentMap();
    this.partitionsToBlockIds = Maps.newConcurrentMap();
  }

  public ShuffleTaskManager(
      ShuffleServerConf conf,
      BufferManager bufferManager,
      ShuffleFlushManager shuffleFlushManager,
      String serverId) {
    requireNonNull(conf);
    requireNonNull(bufferManager);
    this.bufferManager = bufferManager;
    this.conf = conf;
    this.serverId = serverId;
    this.shuffleFlushManager = shuffleFlushManager;
    this.shuffleTaskEngines = Maps.newConcurrentMap();
    this.partitionsToBlockIds = Maps.newConcurrentMap();
  }

  public static String constructKey(String... vars) {
    return String.join(KEY_DELIMITER, vars);
  }

  public StatusCode registerShuffle(String appId, String shuffleId, int startPartition, int endPartition) {
    if (bufferManager.isFull()) {
      return StatusCode.NO_BUFFER;
    }

    ShuffleEngineManager shuffleEngineManager =
        new ShuffleEngineManager(appId, shuffleId, conf, bufferManager, shuffleFlushManager);

    return registerShuffle(appId, shuffleId, startPartition, endPartition, shuffleEngineManager);
  }

  public StatusCode registerShuffle(
      String appId, String shuffleId, int startPartition, int endPartition, ShuffleEngineManager engineManager) {
    String key = constructKey(appId, shuffleId);
    ShuffleEngineManager shuffleEngineManager = shuffleTaskEngines.putIfAbsent(key, engineManager);

    if (shuffleEngineManager == null) {
      ShuffleServerMetrics.gaugeRegisteredShuffle.inc();
      shuffleEngineManager = engineManager;
    }

    return shuffleEngineManager.registerShuffleEngine(startPartition, endPartition);
  }

  public ShuffleEngine getShuffleEngine(String appId, String shuffleId, int partition) {
    String key = constructKey(appId, shuffleId);
    ShuffleEngineManager shuffleEngineManager = shuffleTaskEngines.get(key);

    if (shuffleEngineManager == null) {
      LOG.error("Shuffle task {}~{} {} is not register yet", appId, shuffleId, partition);
      return null;
    }

    return shuffleEngineManager.getShuffleEngine(partition);
  }

  public StatusCode commitShuffle(String appId, String shuffleId) throws Exception {
    String key = constructKey(appId, shuffleId);
    ShuffleEngineManager shuffleEngineManager = shuffleTaskEngines.get(key);

    if (shuffleEngineManager == null) {
      LOG.error("{}~{} try to commit a non-exist shuffle task in this server", appId, shuffleId);
      return StatusCode.NO_REGISTER;
    }

    return shuffleEngineManager.commit();
  }

  public synchronized void addFinishedBlockIds(
      String appId, Integer shuffleId, Map<Integer, List<Long>> partitionToBlockIds) {
    partitionsToBlockIds.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Integer, Map<Integer, List<Long>>> shuffleToPartitions = partitionsToBlockIds.get(appId);
    shuffleToPartitions.putIfAbsent(shuffleId, Maps.newConcurrentMap());
    Map<Integer, List<Long>> existPartitionToBlockIds = shuffleToPartitions.get(shuffleId);
    for (Map.Entry<Integer, List<Long>> entry : partitionToBlockIds.entrySet()) {
      Integer partitionId = entry.getKey();
      existPartitionToBlockIds.putIfAbsent(partitionId, Lists.newArrayList());
      existPartitionToBlockIds.get(partitionId).addAll(entry.getValue());
    }
  }

  public List<Long> getFinishedBlockIds(String appId, Integer shuffleId, Integer partitionId) {
    Map<Integer, Map<Integer, List<Long>>> shuffleToPartitions = partitionsToBlockIds.get(appId);
    if (shuffleToPartitions == null) {
      return Lists.newArrayList();
    }
    Map<Integer, List<Long>> partitionToBlockIds = shuffleToPartitions.get(shuffleId);
    if (partitionToBlockIds == null) {
      return Lists.newArrayList();
    }
    List<Long> blockIds = partitionToBlockIds.get(partitionId);
    if (blockIds == null) {
      return Lists.newArrayList();
    }
    return blockIds;
  }

  @VisibleForTesting
  Map<String, ShuffleEngineManager> getShuffleTaskEngines() {
    return this.shuffleTaskEngines;
  }

  @VisibleForTesting
  void clear() {
    shuffleTaskEngines.clear();
  }

}
