package com.tencent.rss.client.response;

import java.util.List;

public class RssGetShuffleResultResponse extends ClientResponse {

  private List<Long> blockIds;

  public RssGetShuffleResultResponse(ResponseStatusCode statusCode) {
    super(statusCode);
  }

  public List<Long> getBlockIds() {
    return blockIds;
  }

  public void setBlockIds(List<Long> blockIds) {
    this.blockIds = blockIds;
  }
}
