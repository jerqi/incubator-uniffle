package com.tencent.rss.common;

import java.util.List;

public class ShuffleBlockInfo {


    private int partitionId;
    private long blockId;
    private int length;
    private int shuffleId;
    private long crc;
    private byte[] data;
    private List<ShuffleServerInfo> shuffleServerInfos;

    public ShuffleBlockInfo(int shuffleId, int partitionId, long blockId, int length, long crc,
            byte[] data, List<ShuffleServerInfo> shuffleServerInfos) {
        this.partitionId = partitionId;
        this.blockId = blockId;
        this.length = length;
        this.crc = crc;
        this.data = data;
        this.shuffleId = shuffleId;
        this.shuffleServerInfos = shuffleServerInfos;
    }

    public long getBlockId() {
        return blockId;
    }

    public int getLength() {
        return length;
    }

    public long getCrc() {
        return crc;
    }

    public byte[] getData() {
        return data;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public List<ShuffleServerInfo> getShuffleServerInfos() {
        return shuffleServerInfos;
    }
}
