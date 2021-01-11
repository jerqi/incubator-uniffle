package com.tencent.rss.server;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.RssUtils;
import java.util.List;

public class ShuffleDataFlushEvent {

  private long eventId;
  private String appId;
  private String shuffleId;
  private int startPartition;
  private int endPartition;
  private int size;
  private List<ShufflePartitionedBlock> shuffleBlocks;

  public ShuffleDataFlushEvent(
      long eventId,
      String appId,
      String shuffleId,
      int startPartition,
      int endPartition,
      int size,
      List<ShufflePartitionedBlock> shuffleBlocks) {
    this.eventId = eventId;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.size = size;
    this.shuffleBlocks = shuffleBlocks;
  }

  public String getShuffleFilePath() {
    return RssUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition);
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

  @Override
  public String toString() {
    return "ShuffleDataFlushEvent: eventId=" + eventId
        + ", appId=" + appId
        + ", shuffleId=" + shuffleId
        + ", startPartition=" + startPartition
        + ", endPartition=" + endPartition
        + ", blocks=" + shuffleBlocks;
  }
}
