package com.tecent.rss.client;

import com.google.common.collect.Maps;
import com.tencent.rss.common.ShuffleServerGrpcClient;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.Map;

public class ShuffleClientManager {

    private static ShuffleClientManager INSTANCE = new ShuffleClientManager();

    private Map<ShuffleServerInfo, ShuffleServerGrpcClient> clientPool = Maps.newHashMap();

    private ShuffleClientManager() {
    }

    public static ShuffleClientManager getInstance() {
        return INSTANCE;
    }

    public synchronized ShuffleServerGrpcClient getClient(ShuffleServerInfo shuffleServerInfo) {
        if (!clientPool.containsKey(shuffleServerInfo)) {
            ShuffleServerGrpcClient client = new ShuffleServerGrpcClient(
                    shuffleServerInfo.getHost(), shuffleServerInfo.getPort());
            clientPool.put(shuffleServerInfo, client);
        }
        return clientPool.get(shuffleServerInfo);
    }

    public void closeClients() {
        if (clientPool != null) {
            clientPool.values().parallelStream().forEach(client -> client.close());
        }
    }
}
