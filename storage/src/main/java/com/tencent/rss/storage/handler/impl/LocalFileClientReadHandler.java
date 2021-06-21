package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.common.ShuffleDataResult;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileClientReadHandler extends AbstractFileClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileClientReadHandler.class);
  private int partitionNumPerRange;
  private int partitionNum;
  private int readBufferSize;
  private List<ShuffleServerClient> shuffleServerClients;

  public LocalFileClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      List<ShuffleServerClient> shuffleServerClients) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.shuffleServerClients = shuffleServerClients;
  }

  @Override
  public ShuffleDataResult readShuffleData(Set<Long> expectedBlockIds) {
    boolean readSuccessful = false;
    ShuffleDataResult result = null;
    RssGetShuffleDataRequest request = new RssGetShuffleDataRequest(
        appId, shuffleId, partitionId, partitionNumPerRange, partitionNum, readBufferSize, expectedBlockIds);
    for (ShuffleServerClient shuffleServerClient : shuffleServerClients) {
      try {
        RssGetShuffleDataResponse response = shuffleServerClient.getShuffleData(request);
        result = response.getShuffleDataResult();
        readSuccessful = true;
        break;
      } catch (Exception e) {
        LOG.warn("Failed to read shuffle data with " + shuffleServerClient.getClientInfo(), e);
      }
    }
    if (!readSuccessful) {
      throw new RuntimeException("Failed to read shuffle data for appId[" + appId + "], shuffleId["
          + shuffleId + "], partitionId[" + partitionId + "]");
    }
    return result;
  }

  @Override
  public void close() {
  }
}
