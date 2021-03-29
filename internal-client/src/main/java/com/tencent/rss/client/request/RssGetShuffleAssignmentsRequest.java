package com.tencent.rss.client.request;

public class RssGetShuffleAssignmentsRequest {

  private String appId;
  private int shuffleId;
  private int partitionNum;
  private int partitionNumPerRange;
  private int dataReplica;

  public RssGetShuffleAssignmentsRequest(
      String appId, int shuffleId, int partitionNum, int partitionNumPerRange, int dataReplica) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionNum = partitionNum;
    this.partitionNumPerRange = partitionNumPerRange;
    this.dataReplica = dataReplica;
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

  public int getPartitionNumPerRange() {
    return partitionNumPerRange;
  }

  public int getDataReplica() {
    return dataReplica;
  }
}
