package com.tencent.rss.client.response;

import java.util.Set;

public class SendShuffleDataResult {

  private Set<Long> successBlockIds;
  private Set<Long> failedBlockIds;

  public SendShuffleDataResult(Set<Long> successBlockIds, Set<Long> failedBlockIds) {
    this.successBlockIds = successBlockIds;
    this.failedBlockIds = failedBlockIds;
  }

  public Set<Long> getSuccessBlockIds() {
    return successBlockIds;
  }

  public Set<Long> getFailedBlockIds() {
    return failedBlockIds;
  }
}
