package com.tencent.rss.coordinator;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class BasicAssignmentStrategy implements AssignmentStrategy {

  private ClusterManager clusterManager;

  public BasicAssignmentStrategy(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
  }

  public PartitionRangeAssignment assign(int totalPartitionNum, int partitionNumPerServer, int replica) {
    List<PartitionRange> ranges = generateRanges(totalPartitionNum, partitionNumPerServer);
    int hint = ranges.size() * replica;
    List<ServerNode> servers = clusterManager.get(hint);

    if (servers.isEmpty() || servers.size() < replica) {
      return new PartitionRangeAssignment(null);
    }

    SortedMap<PartitionRange, List<ServerNode>> assignments = new TreeMap<>();
    int idx = 0;
    int size = servers.size();

    for (PartitionRange range : ranges) {
      List<ServerNode> nodes = new LinkedList<>();
      for (int i = 0; i < replica; ++i) {
        ServerNode node = servers.get(idx);
        nodes.add(node);
        idx = nextIdx(idx, size);
      }

      assignments.put(range, nodes);
    }

    return new PartitionRangeAssignment(assignments);
  }

  @VisibleForTesting
  int nextIdx(int idx, int size) {
    ++idx;
    if (idx >= size) {
      idx = 0;
    }
    return idx;
  }

  @VisibleForTesting
  List<PartitionRange> generateRanges(int totalPartitionNum, int partitionNumPerServer) {
    List<PartitionRange> ranges = new ArrayList<>();
    if (totalPartitionNum <= 0 || partitionNumPerServer <= 0) {
      return ranges;
    }

    for (int start = 0; start < totalPartitionNum; start += partitionNumPerServer) {
      int end = start + partitionNumPerServer - 1;
      PartitionRange range = new PartitionRange(start, end);
      ranges.add(range);
    }

    return ranges;
  }
}
