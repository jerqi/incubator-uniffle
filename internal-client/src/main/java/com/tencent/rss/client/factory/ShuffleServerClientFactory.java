/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.rss.client.factory;

import java.util.Map;

import com.google.common.collect.Maps;

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.util.ClientType;
import com.tencent.rss.common.ShuffleServerInfo;

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