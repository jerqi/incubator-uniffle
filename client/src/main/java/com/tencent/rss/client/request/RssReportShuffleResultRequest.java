package com.tencent.rss.client.request;

import java.util.List;
import java.util.Map;

public class RssReportShuffleResultRequest {

  private String appId;
  private int shuffleId;
  private Map<Integer, List<Long>> partitionToBlockIds;

  public RssReportShuffleResultRequest(String appId, int shuffleId,
      Map<Integer, List<Long>> partitionToBlockIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionToBlockIds = partitionToBlockIds;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public Map<Integer, List<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }
}
