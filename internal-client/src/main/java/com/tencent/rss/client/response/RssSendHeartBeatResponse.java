package com.tencent.rss.client.response;

import java.util.Set;

public class RssSendHeartBeatResponse extends ClientResponse {

  private Set<String> appIds;

  public RssSendHeartBeatResponse(ResponseStatusCode statusCode) {
    super(statusCode);
  }

  public Set<String> getAppIds() {
    return appIds;
  }

  public void setAppIds(Set<String> appIds) {
    this.appIds = appIds;
  }
}
