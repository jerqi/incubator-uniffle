package com.tencent.rss.common;

import java.nio.ByteBuffer;

public class ShufflePartitionedBlock {

  private int length;
  private long crc;
  private long blockId;
  private int uncompressLength;
  private ByteBuffer data;
  private long taskAttemptId;

  public ShufflePartitionedBlock(
      int length,
      int uncompressLength,
      long crc,
      long blockId,
      long taskAttemptId,
      ByteBuffer data) {
    this.length = length;
    this.crc = crc;
    this.blockId = blockId;
    this.uncompressLength = uncompressLength;
    this.taskAttemptId = taskAttemptId;
    this.data = data;
  }

  public long size() {
    return length;
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
