package com.tencent.rss.common;

public class ShufflePartitionedBlock {

  private int length;
  private long crc;
  private long blockId;
  private int uncompressLength;
  private byte[] data;
  private long taskAttemptId;

  public ShufflePartitionedBlock(
      int length,
      int uncompressLength,
      long crc,
      long blockId,
      long taskAttemptId,
      byte[] data) {
    this.length = length;
    this.crc = crc;
    this.blockId = blockId;
    this.uncompressLength = uncompressLength;
    this.taskAttemptId = taskAttemptId;
    this.data = data;
  }

  // calculate the data size for this block in memory including metadata which are
  // blockId, crc, taskAttemptId, length, uncompressLength
  public long getSize() {
    return length + 3 * 8 + 2 * 4;
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

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public void setTaskAttemptId(long taskAttemptId) {
    this.taskAttemptId = taskAttemptId;
  }

  @Override
  public String toString() {
    return "ShufflePartitionedBlock{blockId[" + blockId + "], length[" + length
        + "], uncompressLength[" + uncompressLength + "], crc[" + crc + "], taskAttemptId[" + taskAttemptId + "]}";
  }

}
