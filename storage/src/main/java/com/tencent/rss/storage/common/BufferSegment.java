package com.tencent.rss.storage.common;

public class BufferSegment {

  private long offset;
  private long length;
  private long crc;

  public BufferSegment(long offset, long length, long crc) {
    this.offset = offset;
    this.length = length;
    this.crc = crc;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BufferSegment) {
      return offset == ((BufferSegment) obj).offset
          && length == ((BufferSegment) obj).length
          && crc == ((BufferSegment) obj).crc;
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
    if (length > Integer.MAX_VALUE) {
      throw new RuntimeException("Unsupported length[" + length + "] for BufferSegment");
    }
    return (int) length;
  }

  public long getCrc() {
    return crc;
  }
}
