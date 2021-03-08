package com.tencent.rss.client.request;

public class RssFinishShuffleRequest {

  private String appId;
  private int shuffleId;

  public RssFinishShuffleRequest(String appId, int shuffleId) {
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
