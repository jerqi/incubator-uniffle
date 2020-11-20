package com.tencent.rss.common;

public class ShuffleBlockInfo {

    private int partitionId;
    private long blockId;
    private int length;
    private long crc;
    private byte[] data;

    public ShuffleBlockInfo(int partitionId, long blockId, int length, long crc, byte[] data) {
        this.partitionId = partitionId;
        this.blockId = blockId;
        this.length = length;
        this.crc = crc;
        this.data = data;
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

    public int getPartitionId() {
        return partitionId;
    }
}
