package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.storage.StorageType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleTaskManager {

    public static final String KEY_DELIMITER = "~";
    private static final Logger logger = LoggerFactory.getLogger(ShuffleEngine.class);
    private static final ShuffleTaskManager INSTANCE = new ShuffleTaskManager();
    protected StorageType storageType = StorageType.HDFS;
    private Map<String, ShuffleEngineManager> shuffleTaskEngines;

    private ShuffleTaskManager() {
        shuffleTaskEngines = new ConcurrentHashMap<>();
    }

    public static String constructKey(String... vars) {
        return String.join(KEY_DELIMITER, vars);
    }

    public static ShuffleTaskManager instance() {
        return INSTANCE;
    }

    public boolean init() {
        return true;
    }

    public StatusCode registerShuffle(String appId, String shuffleId, int startPartition, int endPartition) {
        ShuffleEngineManager shuffleEngineManager = new ShuffleEngineManager(appId, shuffleId);
        return registerShuffle(appId, shuffleId, startPartition, endPartition, shuffleEngineManager);
    }

    public StatusCode registerShuffle(
            String appId, String shuffleId, int startPartition, int endPartition, ShuffleEngineManager engineManager) {
        String key = constructKey(appId, shuffleId);
        ShuffleEngineManager shuffleEngineManager = shuffleTaskEngines.putIfAbsent(key, engineManager);

        if (shuffleEngineManager == null) {
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

    public StatusCode commitShuffle(String appId, String shuffleId) {
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
