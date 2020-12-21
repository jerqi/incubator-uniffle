package com.tencent.rss.coordinator.assignment.strategy;

import static org.junit.Assert.assertEquals;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.coordinator.assignment.PartitionRangeAssignment;
import com.tencent.rss.coordinator.metadata.PartitionRange;
import com.tencent.rss.coordinator.metadata.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.Test;

public class BasicPartitionRangeAssignmentStrategyTest {
    @Test
    public void testStrategy0() {
        final PartitionRangeAssignmentStrategy strategy =
                PartitionRangeAssignmentStrategyFactory.build(new CoordinatorConf());
        final Map<ShuffleServerId, ShuffleServerInfo> serverInfoMap = constructShuffleServers(4);

        final String appId = "application-00000000";
        final int shuffleId = 1;
        final int partitionNum = 39;
        final int partitionPerServer = 6;
        final int replica = 2;
        final PartitionRangeAssignment pra = strategy.assignWithUpdate(serverInfoMap,
                appId, shuffleId, partitionNum, partitionPerServer, replica);

        assertEquals(pra.getAppId(), "application-00000000");
        assertEquals(pra.getShuffleId(), 1);
        SortedMap<PartitionRange, List<String>> assignments = output(pra);
        assertEquals(assignments.toString(), "{PartitionRange[0, 5]=[server-1, server-2], "
                + "PartitionRange[6, 11]=[server-3, server-4], "
                + "PartitionRange[12, 17]=[server-1, server-2], "
                + "PartitionRange[18, 23]=[server-3, server-4], "
                + "PartitionRange[24, 29]=[server-1, server-2], "
                + "PartitionRange[30, 35]=[server-3, server-4], "
                + "PartitionRange[36, 39]=[server-1, server-2]}");
    }

    @Test
    public void testStrategy1() {
        final PartitionRangeAssignmentStrategy strategy =
                PartitionRangeAssignmentStrategyFactory.build(new CoordinatorConf());
        final Map<ShuffleServerId, ShuffleServerInfo> serverInfoMap = constructShuffleServers(1);
        try {
            final PartitionRangeAssignment pra = strategy.assign(serverInfoMap,
                    "application-00000000", 1, 10, 3, 2);
        } catch (Exception e) {
            assertEquals("The number of shuffle server is not enough for replication!", e.getMessage());
        }
    }

    @Test
    public void testStrategy2() {
        final PartitionRangeAssignmentStrategy strategy =
                PartitionRangeAssignmentStrategyFactory.build(new CoordinatorConf());
        final Map<ShuffleServerId, ShuffleServerInfo> serverInfoMap = constructShuffleServers(2);
        final PartitionRangeAssignment pra = strategy.assign(serverInfoMap,
                "application-00000000", 2, 10, 3, 2);
        SortedMap<PartitionRange, List<String>> assignments = output(pra);
        assertEquals(assignments.toString(), "{PartitionRange[0, 2]=[server-1, server-2], "
                + "PartitionRange[3, 5]=[server-1, server-2], "
                + "PartitionRange[6, 8]=[server-1, server-2], "
                + "PartitionRange[9, 10]=[server-1, server-2]}");
    }

    private SortedMap<PartitionRange, List<String>> output(PartitionRangeAssignment pra) {
        SortedMap<PartitionRange, List<String>> assignments = new TreeMap<>();
        for(PartitionRange pr: pra.getAssignments().keySet()) {
            assignments.put(pr, extractServerId(pra.getAssignments().get(pr)));
        }
        return assignments;
    }

    private List<String> extractServerId(Set<ShuffleServerId> servers) {
        return servers.stream().map(ShuffleServerId::getId)
                .sorted(String::compareToIgnoreCase)
                .collect(Collectors.toList());
    }

    private Map<ShuffleServerId, ShuffleServerInfo> constructShuffleServers(int n) {
        Map<ShuffleServerId, ShuffleServerInfo> servers = new HashMap<>();
        for (int i = 1; i <= n; i++) {
            ShuffleServerId shuffleServerId = ShuffleServerId
                    .newBuilder()
                    .setId("server-" + i)
                    .setIp("127.0.0." + i)
                    .setPort(1000)
                    .build();
            ShuffleServerInfo shuffleServerInfo = ShuffleServerInfo.build(shuffleServerId);
            servers.put(shuffleServerId, shuffleServerInfo);
        }
        return servers;
    }
}
