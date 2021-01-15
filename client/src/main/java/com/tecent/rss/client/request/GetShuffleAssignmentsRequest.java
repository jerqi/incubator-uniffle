package com.tecent.rss.client.request;

public class GetShuffleAssignmentsRequest {

  private String appId;
  private int shuffleId;
  private int partitionNum;
  private int partitionsPerServer;

  public GetShuffleAssignmentsRequest(String appId, int shuffleId, int partitionNum, int partitionsPerServer) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionNum = partitionNum;
    this.partitionsPerServer = partitionsPerServer;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public int getPartitionsPerServer() {
    return partitionsPerServer;
  }
}
