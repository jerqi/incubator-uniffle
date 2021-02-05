package com.tencent.rss.client.factory;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.impl.FileBasedShuffleReadClient;
import com.tencent.rss.client.impl.ShuffleWriteClientImpl;
import com.tencent.rss.client.request.CreateShuffleReadClientRequest;
import com.tencent.rss.storage.StorageType;

public class ShuffleClientFactory {

  private static ShuffleClientFactory INSTANCE = new ShuffleClientFactory();

  private ShuffleClientFactory() {
  }

  public static ShuffleClientFactory getINSTANCE() {
    return INSTANCE;
  }

  public ShuffleWriteClient createShuffleWriteClient(String clientType, int retryMax, long retryInterval) {
    return new ShuffleWriteClientImpl(clientType, retryMax, retryInterval);
  }

  public ShuffleReadClient createShuffleReadClient(CreateShuffleReadClientRequest request) {
    if (StorageType.FILE.name().equalsIgnoreCase(request.getStorageType())) {
      return new FileBasedShuffleReadClient(
          request.getBasePath(), request.getHadoopConf(), request.getIndexReadLimit(),
          request.getReadBufferSize(), request.getExpectedBlockIds());
    } else {
      throw new UnsupportedOperationException("Unsupported client type " + request.getStorageType());
    }
  }
}
