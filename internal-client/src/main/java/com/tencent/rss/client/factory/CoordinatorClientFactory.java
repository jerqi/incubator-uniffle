package com.tencent.rss.client.factory;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.client.util.ClientType;

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
