package com.tencent.rss.storage;

import java.util.Objects;

public class HDFSShuffleSegment extends ShuffleSegment {

    private int offset;
    private int length;
    private long crc;
    private long blockId;

    public HDFSShuffleSegment(int offset, int length, long crc, long blockId) {
        this.offset = offset;
        this.length = length;
        this.crc = crc;
        this.blockId = blockId;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HDFSShuffleSegment that = (HDFSShuffleSegment) o;
        return offset == that.offset
                && length == that.length
                && crc == that.crc
                && blockId == that.blockId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, length, crc, blockId);
    }

    @Override
    public String toString() {
        return "HDFSShuffleSegment{"
                + "offset="
                + offset
                + ", length="
                + length
                + ", crc="
                + crc
                + ", blockId='"
                + blockId
                + '\''
                + '}';
    }

}
