package com.tencent.rss.coordinator;

import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.List;
import java.util.stream.Collectors;

public class CoordinatorUtils {

  public static GetShuffleAssignmentsResponse toGetShuffleAssignmentsResponse(
      PartitionRangeAssignment pra, List<ServerNode> serversForResult) {
    List<RssProtos.PartitionRangeAssignment> praList = pra.convertToGrpcProto();
    List<ShuffleServerId> servers = serversForResult.stream()
        .map(ServerNode::convertToGrpcProto)
        .collect(Collectors.toList());

    return GetShuffleAssignmentsResponse.newBuilder()
        .addAllAssignments(praList)
        .addAllServerForResult(servers).build();
  }
}
