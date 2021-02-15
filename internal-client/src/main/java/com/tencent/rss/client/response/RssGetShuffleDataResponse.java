package com.tencent.rss.client.response;

import com.tencent.rss.common.ShuffleDataResult;

public class RssGetShuffleDataResponse extends ClientResponse {

  private ShuffleDataResult shuffleDataResult;

  public RssGetShuffleDataResponse(ResponseStatusCode statusCode) {
    super(statusCode);
  }

  public ShuffleDataResult getShuffleDataResult() {
    return shuffleDataResult;
  }

  public void setShuffleDataResult(ShuffleDataResult shuffleDataResult) {
    this.shuffleDataResult = shuffleDataResult;
  }
}
