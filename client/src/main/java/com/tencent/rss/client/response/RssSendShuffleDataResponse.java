package com.tencent.rss.client.response;

import java.util.List;

public class RssSendShuffleDataResponse extends ClientResponse {

  private List<Long> successBlockIds;
  private List<Long> failedBlockIds;

  public RssSendShuffleDataResponse(ResponseStatusCode statusCode) {
    super(statusCode);
  }

  public List<Long> getSuccessBlockIds() {
    return successBlockIds;
  }

  public void setSuccessBlockIds(List<Long> successBlockIds) {
    this.successBlockIds = successBlockIds;
  }

  public List<Long> getFailedBlockIds() {
    return failedBlockIds;
  }

  public void setFailedBlockIds(List<Long> failedBlockIds) {
    this.failedBlockIds = failedBlockIds;
  }
}
