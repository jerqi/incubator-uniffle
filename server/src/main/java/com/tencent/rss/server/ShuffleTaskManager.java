package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.proto.RssProtos.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class ShuffleTaskManager {

  public static final String KEY_DELIMITER = "~";
  private static final Logger logger = LoggerFactory.getLogger(ShuffleEngine.class);
  private final BufferManager bufferManager;
  private final String serverId;
  private ShuffleServerConf conf;
  private Map<String, ShuffleEngineManager> shuffleTaskEngines;

  public ShuffleTaskManager() {
    this.bufferManager = null;
    this.conf = null;
    this.serverId = "";
    this.shuffleTaskEngines = new ConcurrentHashMap<>();
  }

  public ShuffleTaskManager(ShuffleServerConf conf, BufferManager bufferManager, String serverId) {
    requireNonNull(conf);
    requireNonNull(bufferManager);
    this.bufferManager = bufferManager;
    this.conf = conf;
    this.serverId = serverId;
    this.shuffleTaskEngines = new ConcurrentHashMap<>();
  }

  public static String constructKey(String... vars) {
    return String.join(KEY_DELIMITER, vars);
  }

  public StatusCode registerShuffle(String appId, String shuffleId, int startPartition, int endPartition) {
    if (bufferManager.isFull()) {
      return StatusCode.NO_BUFFER;
    }

    ShuffleEngineManager shuffleEngineManager =
      new ShuffleEngineManager(appId, shuffleId, conf, bufferManager, serverId);

    return registerShuffle(appId, shuffleId, startPartition, endPartition, shuffleEngineManager);
  }

  public StatusCode registerShuffle(
    String appId, String shuffleId, int startPartition, int endPartition, ShuffleEngineManager engineManager) {
    String key = constructKey(appId, shuffleId);
    ShuffleEngineManager shuffleEngineManager = shuffleTaskEngines.putIfAbsent(key, engineManager);

    if (shuffleEngineManager == null) {
      ShuffleServerMetrics.incRegisteredShuffle();
      shuffleEngineManager = engineManager;
    }

    return shuffleEngineManager.registerShuffleEngine(startPartition, endPartition);
  }

  public ShuffleEngine getShuffleEngine(String appId, String shuffleId, int partition) {
    String key = constructKey(appId, shuffleId);
    ShuffleEngineManager shuffleEngineManager = shuffleTaskEngines.get(key);

    if (shuffleEngineManager == null) {
      logger.error("Shuffle task {}~{} {} is not register yet", appId, shuffleId, partition);
      return null;
    }

    return shuffleEngineManager.getShuffleEngine(partition);
  }

  public StatusCode commitShuffle(String appId, String shuffleId) throws IOException, IllegalStateException {
    String key = constructKey(appId, shuffleId);
    ShuffleEngineManager shuffleEngineManager = shuffleTaskEngines.get(key);

    if (shuffleEngineManager == null) {
      logger.error("{}~{} try to commit a non-exist shuffle task in this server", appId, shuffleId);
      return StatusCode.NO_REGISTER;
    }

    return shuffleEngineManager.commit();
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
