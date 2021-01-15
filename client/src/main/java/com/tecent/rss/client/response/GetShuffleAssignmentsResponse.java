package com.tecent.rss.client.response;

import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;

public class GetShuffleAssignmentsResponse extends ClientResponse {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private List<ShuffleRegisterInfo> registerInfoList;

  public GetShuffleAssignmentsResponse(ResponseStatusCode statusCode) {
    super(statusCode);
  }

  public List<ShuffleRegisterInfo> getRegisterInfoList() {
    return registerInfoList;
  }

  public void setRegisterInfoList(List<ShuffleRegisterInfo> registerInfoList) {
    this.registerInfoList = registerInfoList;
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public void setPartitionToServers(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
    this.partitionToServers = partitionToServers;
  }
}
