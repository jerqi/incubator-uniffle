package com.tencent.rss.server;

public class PreAllocatedBufferInfo {

  private long requireId;
  private long timestamp;
  private int requireSize;

  public PreAllocatedBufferInfo(long requireId, long timestamp, int requireSize) {
    this.requireId = requireId;
    this.timestamp = timestamp;
    this.requireSize = requireSize;
  }

  public long getRequireId() {
    return requireId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getRequireSize() {
    return requireSize;
  }
}
