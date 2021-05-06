package com.tencent.rss.client.request;

import java.util.List;
import java.util.Map;

public class RssReportShuffleResultRequest {

  private String appId;
  private int shuffleId;
  private long taskAttemptId;
  private Map<Integer, List<Long>> partitionToBlockIds;

  public RssReportShuffleResultRequest(String appId, int shuffleId, long taskAttemptId,
      Map<Integer, List<Long>> partitionToBlockIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.taskAttemptId = taskAttemptId;
    this.partitionToBlockIds = partitionToBlockIds;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
  }

  public Map<Integer, List<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }
}
