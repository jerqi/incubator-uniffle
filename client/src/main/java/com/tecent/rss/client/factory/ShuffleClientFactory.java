package com.tecent.rss.client.factory;

import com.tecent.rss.client.ShuffleReadClient;
import com.tecent.rss.client.ShuffleWriteClient;
import com.tecent.rss.client.impl.FileBasedShuffleReadClient;
import com.tecent.rss.client.impl.ShuffleWriteClientImpl;
import com.tecent.rss.client.request.CreateShuffleReadClientRequest;
import com.tencent.rss.storage.StorageType;

public class ShuffleClientFactory {

  private static ShuffleClientFactory INSTANCE = new ShuffleClientFactory();

  private ShuffleClientFactory() {
  }

  public static ShuffleClientFactory getINSTANCE() {
    return INSTANCE;
  }

  public ShuffleWriteClient createShuffleWriteClient(String clientType) {
    return new ShuffleWriteClientImpl(clientType);
  }

  public ShuffleReadClient createShuffleReadClient(CreateShuffleReadClientRequest request) {
    if (StorageType.FILE.name().equalsIgnoreCase(request.getStorageType())) {
      return new FileBasedShuffleReadClient(
          request.getBasePath(), request.getHadoopConf(),
          request.getIndexReadLimit(), request.getExpectedBlockIds());
    } else {
      throw new UnsupportedOperationException("Unsupported client type " + request.getStorageType());
    }
  }
}
