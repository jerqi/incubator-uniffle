package com.tencent.rss.coordinator;

public interface AssignmentStrategy {

  PartitionRangeAssignment assign(int totalPartitionNum, int partitionNumPerServer, int replica);
}
