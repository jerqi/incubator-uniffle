package com.tencent.rss.client.factory;

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.util.ClientType;

public class ShuffleServerClientFactory {

  private String clientType;

  public ShuffleServerClientFactory(String clientType) {
    this.clientType = clientType;
  }

  public ShuffleServerClient createShuffleServerClient(String host, int port) {
    if (clientType.equalsIgnoreCase(ClientType.GRPC.name())) {
      return new ShuffleServerGrpcClient(host, port);
    } else {
      throw new UnsupportedOperationException("Unsupported client type " + clientType);
    }
  }

}
