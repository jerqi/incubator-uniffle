package com.tencent.rss.client.response;

public class RssSendCommitResponse extends ClientResponse {

  private int commitCount;

  public RssSendCommitResponse(ResponseStatusCode statusCode) {
    super(statusCode);
  }

  public int getCommitCount() {
    return commitCount;
  }

  public void setCommitCount(int commitCount) {
    this.commitCount = commitCount;
  }
}
