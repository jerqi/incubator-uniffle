package com.tecent.rss.client.response;

import java.util.List;

public class SendShuffleDataResult {

  private List<Long> successBlockIds;
  private List<Long> failedBlockIds;

  public SendShuffleDataResult(List<Long> successBlockIds, List<Long> failedBlockIds) {
    this.successBlockIds = successBlockIds;
    this.failedBlockIds = failedBlockIds;
  }

  public List<Long> getSuccessBlockIds() {
    return successBlockIds;
  }

  public List<Long> getFailedBlockIds() {
    return failedBlockIds;
  }
}
