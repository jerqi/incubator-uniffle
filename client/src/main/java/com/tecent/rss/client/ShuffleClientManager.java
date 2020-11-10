package com.tecent.rss.client;

import java.util.HashMap;
import java.util.Map;

public class ShuffleClientManager {

    private static ShuffleClientManager INSTANCE = new ShuffleClientManager();

    private Map<Integer, ShuffleClient> clientPool = new HashMap<>();

    private ShuffleClientManager() {
    }

    public ShuffleClientManager getInstance() {
        return INSTANCE;
    }

    public void initClientPool() {
    }

    public void getClient(int partitionId) {
    }

    public void closeClients() {
    }
}
