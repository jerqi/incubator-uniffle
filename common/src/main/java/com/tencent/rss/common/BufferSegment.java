package com.tencent.rss.common;

public class BufferSegment {

  private long blockId;
  private long offset;
  private int length;
  private int uncompressLength;
  private long crc;

  public BufferSegment(long blockId, long offset, int length, int uncompressLength, long crc) {
    this.blockId = blockId;
    this.offset = offset;
    this.length = length;
    this.uncompressLength = uncompressLength;
    this.crc = crc;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BufferSegment) {
      return blockId == ((BufferSegment) obj).getBlockId()
          && offset == ((BufferSegment) obj).getOffset()
          && length == ((BufferSegment) obj).getLength()
          && uncompressLength == ((BufferSegment) obj).getUncompressLength()
          && crc == ((BufferSegment) obj).getCrc();
    }
    return false;
  }

  public int getOffset() {
    if (offset > Integer.MAX_VALUE) {
      throw new RuntimeException("Unsupported offset[" + offset + "] for BufferSegment");
    }
    return (int) offset;
  }

  public int getLength() {
    return length;
  }

  public long getBlockId() {
    return blockId;
  }

  public long getCrc() {
    return crc;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }
}
