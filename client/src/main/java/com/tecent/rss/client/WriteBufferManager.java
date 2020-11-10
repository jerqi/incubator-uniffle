package com.tecent.rss.client;

import com.tencent.rss.proto.RssProtos;
import java.util.HashMap;
import java.util.Map;

public class WriteBufferManager {

    // cache partition -> records
    private Map<Integer, byte[]> buffers = new HashMap<>();

    // add record to cache, return [partition, ShuffleBlock] if meet spill condition
    public Map<Integer, RssProtos.ShuffleBlock> addRecord(int partitionId, Object key, Object value) {
        return null;
    }

    // transform all [partition, records] to [partition, ShuffleBlock] and clear cache
    public Map<Integer, RssProtos.ShuffleBlock> clear() {
        return null;
    }

    // transform records to shuffleBlock
    private RssProtos.ShuffleBlock createShuffleBlock() {
        return null;
    }
}
