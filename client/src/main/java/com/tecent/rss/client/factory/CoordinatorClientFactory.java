package com.tecent.rss.client.factory;

import com.tecent.rss.client.ClientType;
import com.tecent.rss.client.CoordinatorClient;
import com.tecent.rss.client.impl.grpc.CoordinatorGrpcClient;

public class CoordinatorClientFactory {

  private String clientType;

  public CoordinatorClientFactory(String clientType) {
    this.clientType = clientType;
  }

  public CoordinatorClient createCoordinatorClient(String host, int port) {
    if (clientType.equalsIgnoreCase(ClientType.GRPC.name())) {
      return new CoordinatorGrpcClient(host, port);
    } else {
      throw new UnsupportedOperationException("Unsupported client type " + clientType);
    }
  }
}
