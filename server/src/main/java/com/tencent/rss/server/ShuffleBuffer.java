package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import com.tencent.rss.proto.RssProtos.ShuffleData;
import com.tencent.rss.proto.RssProtos.StatusCode;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ShuffleBuffer {

    // blockId is int64, length is int32 and crc is int64
    private static final int BLOCK_HEAD_SIZE = 8 + 4 + 8;
    private int capacity;
    private int ttl;
    private int start; // start partition
    private int end; // end partition
    private int size;
    private Map<Integer, List<ShuffleBlock>> partitionBuffers;

    public ShuffleBuffer(int capacity, int ttl, int start, int end) {
        this.capacity = capacity;
        this.ttl = ttl;
        this.start = start;
        this.end = end;

        partitionBuffers = new HashMap<>();
        for (int i = start; i <= end; ++i) {
            partitionBuffers.put(i, new LinkedList<>());
        }
    }

    public void clear() {
        if (partitionBuffers != null) {
            partitionBuffers.forEach((k, v) -> v.clear());
        }
    }

    public StatusCode append(ShuffleData data) {
        int partition = data.getPartitionId();
        List<ShuffleBlock> cur = partitionBuffers.get(partition);

        for (ShuffleBlock block : data.getBlockList()) {
            cur.add(block);
            size += block.getLength() + BLOCK_HEAD_SIZE;
        }

        return StatusCode.SUCCESS;
    }

    public List<ShuffleBlock> getBlocks(int partition) {
        return partitionBuffers.get(partition);
    }

    public int getSize() {
        return this.size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean full() {
        return this.size > capacity;
    }

    @VisibleForTesting
    int getCapacity() {
        return this.capacity;
    }
}
