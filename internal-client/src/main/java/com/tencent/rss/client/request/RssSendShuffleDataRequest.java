package com.tencent.rss.client.request;

import com.tencent.rss.common.ShuffleBlockInfo;
import java.util.List;
import java.util.Map;

public class RssSendShuffleDataRequest {

  private String appId;
  private int retryMax;
  private long retryInterval;
  private Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks;

  public RssSendShuffleDataRequest(String appId, int retryMax, long retryInterval,
      Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks) {
    this.appId = appId;
    this.retryMax = retryMax;
    this.retryInterval = retryInterval;
    this.shuffleIdToBlocks = shuffleIdToBlocks;
  }

  public String getAppId() {
    return appId;
  }

  public int getRetryMax() {
    return retryMax;
  }

  public long getRetryInterval() {
    return retryInterval;
  }

  public Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> getShuffleIdToBlocks() {
    return shuffleIdToBlocks;
  }
}
