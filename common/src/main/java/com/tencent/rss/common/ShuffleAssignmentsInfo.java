package com.tencent.rss.common;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShuffleAssignmentsInfo {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private Set<ShuffleServerInfo> shuffleServersForResult;
  private Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges;

  public ShuffleAssignmentsInfo(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges,
      Set<ShuffleServerInfo> shuffleServersForResult) {
    this.partitionToServers = partitionToServers;
    this.serverToPartitionRanges = serverToPartitionRanges;
    this.shuffleServersForResult = shuffleServersForResult;
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public Set<ShuffleServerInfo> getShuffleServersForResult() {
    return shuffleServersForResult;
  }

  public Map<ShuffleServerInfo, List<PartitionRange>> getServerToPartitionRanges() {
    return serverToPartitionRanges;
  }
}
