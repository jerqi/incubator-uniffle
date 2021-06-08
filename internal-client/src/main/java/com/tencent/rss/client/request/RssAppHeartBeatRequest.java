package com.tencent.rss.client.request;

public class RssAppHeartBeatRequest {

  private final String appId;
  private final long timeoutMs;

  public RssAppHeartBeatRequest(String appId, long timeoutMs) {
    this.appId = appId;
    this.timeoutMs = timeoutMs;
  }

  public String getAppId() {
    return appId;
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }
}
