package com.tecent.rss.client.factory;

import com.tecent.rss.client.ClientType;
import com.tecent.rss.client.ShuffleServerClient;
import com.tecent.rss.client.impl.grpc.ShuffleServerGrpcClient;

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
