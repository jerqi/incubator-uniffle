package com.tencent.rss.client.response;

import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;

public class ShuffleAssignmentsInfo {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private List<ShuffleRegisterInfo> registerInfoList;

  public ShuffleAssignmentsInfo(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      List<ShuffleRegisterInfo> registerInfoList) {
    this.partitionToServers = partitionToServers;
    this.registerInfoList = registerInfoList;
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public List<ShuffleRegisterInfo> getRegisterInfoList() {
    return registerInfoList;
  }
}
