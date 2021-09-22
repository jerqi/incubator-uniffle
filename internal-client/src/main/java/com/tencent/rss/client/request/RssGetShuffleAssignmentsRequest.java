package com.tencent.rss.client.request;

import java.util.Set;

public class RssGetShuffleAssignmentsRequest {

  private String appId;
  private int shuffleId;
  private int partitionNum;
  private int partitionNumPerRange;
  private int dataReplica;
  private Set<String> requiredTags;

  public RssGetShuffleAssignmentsRequest(String appId, int shuffleId, int partitionNum,
      int partitionNumPerRange, int dataReplica, Set<String> requiredTags) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionNum = partitionNum;
    this.partitionNumPerRange = partitionNumPerRange;
    this.dataReplica = dataReplica;
    this.requiredTags = requiredTags;
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

  public Set<String> getRequiredTags() {
    return requiredTags;
  }
}
