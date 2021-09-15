package com.tencent.rss.server;

import com.sun.org.apache.xpath.internal.operations.Bool;
import com.tencent.rss.common.ShufflePartitionedBlock;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class ShuffleDataFlushEvent {

  private long eventId;
  private String appId;
  private int shuffleId;
  private int startPartition;
  private int endPartition;
  private long size;
  private List<ShufflePartitionedBlock> shuffleBlocks;
  private Supplier<Boolean> valid = null;

  public ShuffleDataFlushEvent(
      long eventId,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      long size,
      List<ShufflePartitionedBlock> shuffleBlocks) {
    this.eventId = eventId;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.size = size;
    this.shuffleBlocks = shuffleBlocks;
  }

  public ShuffleDataFlushEvent(
      long eventId,
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      long size,
      List<ShufflePartitionedBlock> shuffleBlocks,
      Supplier<Boolean> valid) {
    this.eventId = eventId;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.size = size;
    this.shuffleBlocks = shuffleBlocks;
    this.valid = valid;
  }

  public List<ShufflePartitionedBlock> getShuffleBlocks() {
    return shuffleBlocks;
  }

  public long getEventId() {
    return eventId;
  }

  public long getSize() {
    return size;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStartPartition() {
    return startPartition;
  }

  public int getEndPartition() {
    return endPartition;
  }

  public boolean isValid() {
    if (valid == null) {
      return true;
    }
    return valid.get();
  }

  @Override
  public String toString() {
    return "ShuffleDataFlushEvent: eventId=" + eventId
        + ", appId=" + appId
        + ", shuffleId=" + shuffleId
        + ", startPartition=" + startPartition
        + ", endPartition=" + endPartition;
  }
}
