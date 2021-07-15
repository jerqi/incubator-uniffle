package com.tencent.rss.client.request;

import com.tencent.rss.common.PartitionRange;
import java.util.List;

public class RssRegisterShuffleRequest {

  private String appId;
  private int shuffleId;
  private List<PartitionRange> partitionRanges;

  public RssRegisterShuffleRequest(String appId, int shuffleId, List<PartitionRange> partitionRanges) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionRanges = partitionRanges;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public List<PartitionRange> getPartitionRanges() {
    return partitionRanges;
  }
}
