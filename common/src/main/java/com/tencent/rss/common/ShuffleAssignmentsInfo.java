package com.tencent.rss.common;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShuffleAssignmentsInfo {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private List<ShuffleRegisterInfo> registerInfoList;
  private Set<ShuffleServerInfo> shuffleServersForResult;

  public ShuffleAssignmentsInfo(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      List<ShuffleRegisterInfo> registerInfoList,
      Set<ShuffleServerInfo> shuffleServersForResult) {
    this.partitionToServers = partitionToServers;
    this.registerInfoList = registerInfoList;
    this.shuffleServersForResult = shuffleServersForResult;
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public List<ShuffleRegisterInfo> getRegisterInfoList() {
    return registerInfoList;
  }

  public Set<ShuffleServerInfo> getShuffleServersForResult() {
    return shuffleServersForResult;
  }
}
