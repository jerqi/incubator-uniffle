package com.tencent.rss.client.request;

import com.tencent.rss.common.ShuffleBlockInfo;
import java.util.List;
import java.util.Map;

public class RssSendShuffleDataRequest {

  private String appId;
  private Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks;

  public RssSendShuffleDataRequest(String appId,
      Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks) {
    this.appId = appId;
    this.shuffleIdToBlocks = shuffleIdToBlocks;
  }

  public String getAppId() {
    return appId;
  }

  public Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> getShuffleIdToBlocks() {
    return shuffleIdToBlocks;
  }
}
