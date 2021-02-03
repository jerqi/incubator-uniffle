package com.tencent.rss.client.factory;

import com.google.common.collect.Maps;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.util.ClientType;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.Map;

public class ShuffleServerClientFactory {

  private static ShuffleServerClientFactory INSTANCE;
  private Map<String, Map<ShuffleServerInfo, ShuffleServerClient>> clients;

  private ShuffleServerClientFactory() {
    clients = Maps.newConcurrentMap();
  }

  public static synchronized ShuffleServerClientFactory getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new ShuffleServerClientFactory();
    }
    return INSTANCE;
  }

  private ShuffleServerClient createShuffleServerClient(String clientType, ShuffleServerInfo shuffleServerInfo) {
    if (clientType.equalsIgnoreCase(ClientType.GRPC.name())) {
      return new ShuffleServerGrpcClient(shuffleServerInfo.getHost(), shuffleServerInfo.getPort());
    } else {
      throw new UnsupportedOperationException("Unsupported client type " + clientType);
    }
  }

  public synchronized ShuffleServerClient getShuffleServerClient(
      String clientType, ShuffleServerInfo shuffleServerInfo) {
    clients.putIfAbsent(clientType, Maps.newConcurrentMap());
    Map<ShuffleServerInfo, ShuffleServerClient> serverToClients = clients.get(clientType);
    if (serverToClients.get(shuffleServerInfo) == null) {
      serverToClients.put(shuffleServerInfo, createShuffleServerClient(clientType, shuffleServerInfo));
    }
    return serverToClients.get(shuffleServerInfo);
  }
}
