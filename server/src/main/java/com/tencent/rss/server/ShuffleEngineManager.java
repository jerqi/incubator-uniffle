package com.tencent.rss.server;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.tencent.rss.proto.RssProtos.StatusCode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleEngineManager {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleEngineManager.class);

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

    public StatusCode registerShuffleEngine(int startPartition, int endPartition) {
        ShuffleEngine engine = new ShuffleEngine(appId, shuffleId, startPartition, endPartition);
        return registerShuffleEngine(startPartition, endPartition, engine);
    }

    public StatusCode registerShuffleEngine(int startPartition, int endPartition, ShuffleEngine engine) {
        String key = ShuffleTaskManager.constructKey(String.valueOf(startPartition), String.valueOf(endPartition));

        engineMap.putIfAbsent(key, engine);
        synchronized (this) {
            partitionRangeMap.put(Range.closed(startPartition, endPartition), key);
        }
        ShuffleEngine shuffleEngine = engineMap.get(key);

        return shuffleEngine.init();
    }

    public ShuffleEngine getShuffleEngine(int partition) {
        String key = partitionRangeMap.get(partition);
        if (key == null) {
            logger.error("{}~{} Can't find shuffle engine of partition {} from range map", appId, shuffleId, partition);
            return null;
        }

        ShuffleEngine shuffleEngine = engineMap.get(key);
        if (shuffleEngine == null) {
            logger.error("{}~{} Can't find shuffle engine of partition {}from engine map", appId, shuffleId, partition);
        }

        return shuffleEngine;
    }

    public StatusCode commit() {
        synchronized (this) {
            if (isCommitted) {
                return StatusCode.SUCCESS;
            }

            engineMap.forEach((k, v) -> {
                v.flush();
            });
            isCommitted = true;
            return StatusCode.SUCCESS;
        }
    }

}
