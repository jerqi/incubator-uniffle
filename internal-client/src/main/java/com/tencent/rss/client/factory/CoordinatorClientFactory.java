package com.tencent.rss.client.factory;

import com.google.common.collect.Lists;
import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.client.util.ClientType;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorClientFactory.class);

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

  public List<CoordinatorClient> createCoordinatorClient(String coordinators) {
    LOG.info("Start to create coordinator clients from {}", coordinators);
    List<CoordinatorClient> coordinatorClients = Lists.newLinkedList();
    String[] coordinatorList = coordinators.trim().split(",");
    if (coordinatorList.length <= 0) {
      String msg = "Invalid " + coordinators;
      LOG.error(msg);
      throw new RuntimeException(msg);
    }

    for (String coordinator: coordinatorList) {
      String[] ipPort = coordinator.trim().split(":");
      if (ipPort.length != 2) {
        String msg = "Invalid coordinator format " + ipPort;
        LOG.error(msg);
        throw new RuntimeException(msg);
      }

      String host = ipPort[0];
      int port = Integer.parseInt(ipPort[1]);
      CoordinatorClient coordinatorClient = createCoordinatorClient(host, port);
      coordinatorClients.add(coordinatorClient);
      LOG.info("Add coordinator client {}", coordinatorClient.getDesc());
    }
    LOG.info("Finish create coordinator clients {}",
        coordinatorClients.stream().map(CoordinatorClient::getDesc).collect(Collectors.joining(", ")));
    return coordinatorClients;
  }
}
