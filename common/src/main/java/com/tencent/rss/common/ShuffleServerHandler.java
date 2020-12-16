package com.tencent.rss.common;

import java.util.List;
import java.util.Map;

public class ShuffleServerHandler {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

  public ShuffleServerHandler(
    Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
    this.partitionToServers = partitionToServers;
  }

  public List<ShuffleServerInfo> getShuffleServers(int partition) {
    return partitionToServers.get(partition);
  }
}
