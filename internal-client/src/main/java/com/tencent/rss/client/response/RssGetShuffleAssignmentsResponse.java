package com.tencent.rss.client.response;

import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;

public class RssGetShuffleAssignmentsResponse extends ClientResponse {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
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

  public Map<ShuffleServerInfo, List<PartitionRange>> getServerToPartitionRanges() {
    return serverToPartitionRanges;
  }

  public void setServerToPartitionRanges(
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges) {
    this.serverToPartitionRanges = serverToPartitionRanges;
  }
}
