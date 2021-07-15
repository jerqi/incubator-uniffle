package com.tencent.rss.client.response;

import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RssGetShuffleAssignmentsResponse extends ClientResponse {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private Set<ShuffleServerInfo> shuffleServersForResult;
  private Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges;

  public RssGetShuffleAssignmentsResponse(ResponseStatusCode statusCode) {
    super(statusCode);
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

  public Map<ShuffleServerInfo, List<PartitionRange>> getServerToPartitionRanges() {
    return serverToPartitionRanges;
  }

  public void setServerToPartitionRanges(
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges) {
    this.serverToPartitionRanges = serverToPartitionRanges;
  }
}
