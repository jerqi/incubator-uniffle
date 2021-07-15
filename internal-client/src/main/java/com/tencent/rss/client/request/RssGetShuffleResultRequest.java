package com.tencent.rss.client.request;

public class RssGetShuffleResultRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;

  public RssGetShuffleResultRequest(String appId, int shuffleId, int partitionId) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }
}
