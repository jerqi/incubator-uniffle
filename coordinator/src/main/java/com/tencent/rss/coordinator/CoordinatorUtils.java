package com.tencent.rss.coordinator;

import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;

import java.util.List;

public class CoordinatorUtils {

  public static GetShuffleAssignmentsResponse toGetShuffleAssignmentsResponse(
      PartitionRangeAssignment pra) {
    List<RssProtos.PartitionRangeAssignment> praList = pra.convertToGrpcProto();

    return GetShuffleAssignmentsResponse.newBuilder()
        .addAllAssignments(praList)
        .build();
  }
}
