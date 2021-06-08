package com.tencent.rss.client.factory;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.ShuffleWriteClientImpl;
import com.tencent.rss.client.request.CreateShuffleReadClientRequest;

public class ShuffleClientFactory {

  private static final ShuffleClientFactory INSTANCE = new ShuffleClientFactory();

  private ShuffleClientFactory() {
  }

  public static ShuffleClientFactory getInstance() {
    return INSTANCE;
  }

  public ShuffleWriteClient createShuffleWriteClient(
      String clientType, int retryMax, long retryIntervalMax, int heartBeatThreadNum) {
    return new ShuffleWriteClientImpl(clientType, retryMax, retryIntervalMax, heartBeatThreadNum);
  }

  public ShuffleReadClient createShuffleReadClient(CreateShuffleReadClientRequest request) {
    return new ShuffleReadClientImpl(request.getStorageType(), request.getAppId(), request.getShuffleId(),
        request.getPartitionId(), request.getIndexReadLimit(), request.getPartitionNumPerRange(),
        request.getPartitionNum(), request.getReadBufferSize(), request.getBasePath(),
        request.getExpectedBlockIds(), request.getShuffleServerInfoList(), request.getHadoopConf());
  }
}
