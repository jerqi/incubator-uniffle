package com.tencent.rss.client.request;

import java.util.List;

public class RssGetShuffleResultRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private List<Long> taskAttemptIds;

  public RssGetShuffleResultRequest(String appId, int shuffleId, int partitionId, List<Long> taskAttemptIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.taskAttemptIds = taskAttemptIds;
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

  public List<Long> getTaskAttemptIds() {
    return taskAttemptIds;
  }
}
