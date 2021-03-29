package com.tencent.rss.coordinator;

import java.util.List;

public interface AssignmentStrategy {

  PartitionRangeAssignment assign(int totalPartitionNum, int partitionNumPerRange, int replica);

  List<ServerNode> assignServersForResult(int replica);
}
