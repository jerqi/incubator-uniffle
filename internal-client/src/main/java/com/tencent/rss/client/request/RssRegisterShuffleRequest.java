package com.tencent.rss.client.request;

public class RssRegisterShuffleRequest {

  private String appId;
  private int shuffleId;
  private int start;
  private int end;

  public RssRegisterShuffleRequest(String appId, int shuffleId, int start, int end) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.start = start;
    this.end = end;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }
}
