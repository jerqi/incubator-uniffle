package com.tencent.rss.common;

import java.io.Serializable;

public class ShuffleServerInfo implements Serializable {

    private String id;

    private String address;

    private int port;

    public ShuffleServerInfo(String id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    public String getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ShuffleServerInfo) {
            return id.equals(((ShuffleServerInfo) obj).getId())
                    && address.equals(((ShuffleServerInfo) obj).getAddress())
                    && port == ((ShuffleServerInfo) obj).getPort();
        }
        return false;
    }
}
