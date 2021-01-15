package com.tecent.rss.client.request;

public class SendCommitRequest {

  private String appId;
  private int shuffleId;

  public SendCommitRequest(String appId, int shuffleId) {
    this.appId = appId;
    this.shuffleId = shuffleId;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }
}
