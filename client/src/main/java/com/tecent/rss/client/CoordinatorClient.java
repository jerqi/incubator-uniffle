package com.tecent.rss.client;

import com.tencent.rss.common.ShuffleServerHandler;

public class CoordinatorClient {

    private String coordinatorAddress;

    public CoordinatorClient(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
    }

    public boolean isRssAvailable() {
        return false;
    }

    public ShuffleServerHandler[] getShuffleServers() {
        return null;
    }
}
