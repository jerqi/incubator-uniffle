package com.tecent.rss.client.response;

import java.util.List;

public class SendShuffleDataResponse extends ClientResponse {

  private List<Long> successBlockIds;
  private List<Long> failedBlockIds;

  public SendShuffleDataResponse(ResponseStatusCode statusCode) {
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
