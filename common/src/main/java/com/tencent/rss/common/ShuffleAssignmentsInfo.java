package com.tencent.rss.common;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShuffleAssignmentsInfo {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges;

  public ShuffleAssignmentsInfo(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges) {
    this.partitionToServers = partitionToServers;
    this.serverToPartitionRanges = serverToPartitionRanges;
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public Map<ShuffleServerInfo, List<PartitionRange>> getServerToPartitionRanges() {
    return serverToPartitionRanges;
  }
}
