package com.tencent.rss.common;

import java.nio.ByteBuffer;
import java.util.Objects;

public class ShufflePartitionedBlock {

    private int length;
    private long crc;
    private long blockId;
    private ByteBuffer data;

    public ShufflePartitionedBlock(int length, long crc, long blockId, ByteBuffer data) {
        this(length, crc, blockId);
        Objects.requireNonNull(data);
        this.data = data;
    }

    public ShufflePartitionedBlock(int length, long crc, long blockId, byte[] data) {
        this(length, crc, blockId);
        Objects.requireNonNull(data);
        this.data = ByteBuffer.wrap(data);
    }

    public ShufflePartitionedBlock(int length, long crc, long blockId) {
        this.length = length;
        this.crc = crc;
        this.blockId = blockId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShufflePartitionedBlock that = (ShufflePartitionedBlock) o;
        return length == that.length
                && crc == that.crc
                && blockId == that.blockId
                && data.equals(that.data);
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public long getCrc() {
        return crc;
    }

    public void setCrc(long crc) {
        this.crc = crc;
    }

    public long getBlockId() {
        return blockId;
    }

    public void setBlockId(long blockId) {
        this.blockId = blockId;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }


}
