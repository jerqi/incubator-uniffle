package com.tencent.rss.common;

import java.io.Serializable;

public class ShuffleServerInfo implements Serializable {

  private String id;

  private String host;

  private int port;

  public ShuffleServerInfo(String id, String host, int port) {
    this.id = id;
    this.host = host;
    this.port = port;
  }

  public String getId() {
    return id;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public int hashCode() {
    return host.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ShuffleServerInfo) {
      return id.equals(((ShuffleServerInfo) obj).getId())
          && host.equals(((ShuffleServerInfo) obj).getHost())
          && port == ((ShuffleServerInfo) obj).getPort();
    }
    return false;
  }

  @Override
  public String toString() {
    return "ShuffleServerInfo{id[" + id + "], host[" + host + "], port[" + port + "]}";
  }
}
