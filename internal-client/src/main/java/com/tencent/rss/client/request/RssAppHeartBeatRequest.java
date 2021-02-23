package com.tencent.rss.client.request;

public class RssAppHeartBeatRequest {

  private String appId;

  public RssAppHeartBeatRequest(String appId) {
    this.appId = appId;
  }

  public String getAppId() {
    return appId;
  }
}
