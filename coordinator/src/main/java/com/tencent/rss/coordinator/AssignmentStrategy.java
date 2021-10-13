package com.tencent.rss.coordinator;

import java.util.List;
import java.util.Set;

public interface AssignmentStrategy {

  PartitionRangeAssignment assign(int totalPartitionNum, int partitionNumPerRange,
      int replica, Set<String> requiredTags);

}
