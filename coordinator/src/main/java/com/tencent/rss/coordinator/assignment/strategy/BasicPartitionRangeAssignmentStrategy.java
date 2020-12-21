package com.tencent.rss.coordinator.assignment.strategy;

import com.tencent.rss.coordinator.assignment.PartitionRangeAssignment;
import com.tencent.rss.coordinator.metadata.ApplicationInfo;
import com.tencent.rss.coordinator.metadata.PartitionRange;
import com.tencent.rss.coordinator.metadata.ShuffleInfo;
import com.tencent.rss.coordinator.metadata.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Basic strategy that chooses the shuffle servers with most minimum number of partition range
 */
class BasicPartitionRangeAssignmentStrategy implements PartitionRangeAssignmentStrategy {

    @Override
    public PartitionRangeAssignment assign(
            Map<ShuffleServerId, ShuffleServerInfo> shuffleServerInfoMap,
            String appId,
            int shuffleId,
            int partitionNum,
            int partitionPerServer,
            int replica) {
        if (shuffleServerInfoMap.size() < replica) {
            throw new IllegalArgumentException("The number of shuffle server is not enough for replication!");
        }
        final SortedMap<PartitionRange, Set<ShuffleServerId>> assignments = new TreeMap<>();
        final Map<ShuffleServerId, Set<PartitionRange>> reversedAssignments = new HashMap<>();

        final Map<ShuffleServerId, Integer> serverPartitionRanges = new HashMap<>();
        for (ShuffleServerId shuffleServerId: shuffleServerInfoMap.keySet()) {
            int partitionRanges = shuffleServerInfoMap.get(shuffleServerId).countPartitionRanges();
            serverPartitionRanges.put(shuffleServerId, partitionRanges);
        }

        final int prNum = partitionNum % partitionPerServer == 0
                ? partitionNum / partitionPerServer : partitionNum / partitionPerServer + 1;

        for (int i = 0; i < prNum; i++) {
            int start = i * partitionPerServer;
            int end = Math.min(partitionNum, (i + 1) * partitionPerServer - 1);
            PartitionRange pr = PartitionRange.build(start, end);
            Set<ShuffleServerId> servers = findTopK(serverPartitionRanges, replica);

            assignments.put(pr, servers);
            for (ShuffleServerId server: servers) {
                if (reversedAssignments.containsKey(server)) {
                    reversedAssignments.get(server).add(pr);
                } else {
                    Set<PartitionRange> prSet = new HashSet<>();
                    prSet.add(pr);
                    reversedAssignments.put(server, prSet);
                }
                int cnt = serverPartitionRanges.get(server);
                serverPartitionRanges.put(server, cnt + 1);
            }
        }
        return new PartitionRangeAssignment(appId, shuffleId, assignments, reversedAssignments);
    }

    @Override
    public PartitionRangeAssignment assignWithUpdate(
            Map<ShuffleServerId, ShuffleServerInfo> shuffleServerInfoMap,
            String appId, int shuffleId, int partitionNum, int partitionPerServer, int replica) {
        final PartitionRangeAssignment pra = assign(shuffleServerInfoMap,
            appId, shuffleId, partitionNum, partitionPerServer, replica);
        final Map<ShuffleServerId, Set<PartitionRange>> reversedAssignments = pra.getReversedAssignments();
        for (Entry<ShuffleServerId, Set<PartitionRange>> entry: reversedAssignments.entrySet()) {
            ApplicationInfo applicationInfo = ApplicationInfo.build(appId);
            ShuffleInfo shuffleInfo = ShuffleInfo.build(shuffleId);
            shuffleInfo.addAllPartitionRanges(entry.getValue());
            applicationInfo.addShuffleInfo(shuffleInfo);
            shuffleServerInfoMap.get(entry.getKey()).addApplicationInfo(applicationInfo);
        }
        return pra;
    }

    private Set<ShuffleServerId> findTopK(Map<ShuffleServerId, Integer> serverPartitions, int k) {
        List<Entry<ShuffleServerId, Integer>> entries = new ArrayList<>(serverPartitions.entrySet());
        Comparator<Entry<ShuffleServerId, Integer>> comparator = (o1, o2) -> {
            if (o1.getValue().equals(o2.getValue())) {
                return o1.getKey().getId().compareToIgnoreCase(o2.getKey().getId());
            }
            return o1.getValue() - o2.getValue();
        };
        entries.sort(comparator);
        Set<ShuffleServerId> results = new HashSet<>();
        for (int i = 0; i < k; i++) {
            results.add(entries.get(i).getKey());
        }
        return results;
    }
}
