package com.tencent.rss.common;

public class BufferSegment {

  private long blockId;
  private long offset;
  private int length;
  private int uncompressLength;
  private long crc;
  private long taskAttemptId;

  public BufferSegment(long blockId, long offset, int length, int uncompressLength, long crc, long taskAttemptId) {
    this.blockId = blockId;
    this.offset = offset;
    this.length = length;
    this.uncompressLength = uncompressLength;
    this.crc = crc;
    this.taskAttemptId = taskAttemptId;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BufferSegment) {
      return blockId == ((BufferSegment) obj).getBlockId()
          && offset == ((BufferSegment) obj).getOffset()
          && length == ((BufferSegment) obj).getLength()
          && uncompressLength == ((BufferSegment) obj).getUncompressLength()
          && crc == ((BufferSegment) obj).getCrc()
          && taskAttemptId == ((BufferSegment) obj).getTaskAttemptId();
    }
    return false;
  }

  @Override
  public String toString() {
    return "BufferSegment{blockId[" + blockId + "], taskAttemptId[" + taskAttemptId
        + "], offset[" + offset + "], length[" + length + "], crc[" + crc
        + "], uncompressLength[" + uncompressLength + "]}";
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

  public long getTaskAttemptId() {
    return taskAttemptId;
  }
}
