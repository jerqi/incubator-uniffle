package com.tencent.rss.client.response;

import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RssGetShuffleAssignmentsResponse extends ClientResponse {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private List<ShuffleRegisterInfo> registerInfoList;
  private Set<ShuffleServerInfo> shuffleServersForResult;

  public RssGetShuffleAssignmentsResponse(ResponseStatusCode statusCode) {
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

  public Set<ShuffleServerInfo> getShuffleServersForResult() {
    return shuffleServersForResult;
  }

  public void setShuffleServersForResult(Set<ShuffleServerInfo> shuffleServersForResult) {
    this.shuffleServersForResult = shuffleServersForResult;
  }
}
