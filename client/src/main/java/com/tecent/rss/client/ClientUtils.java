package com.tecent.rss.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.PartitionRangeAssignment;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ClientUtils {

    private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

    // BlockId is long and composed by executorId and AtomicInteger
    // executorId is high-32 bit and AtomicInteger is low-32 bit
    public static Long getBlockId(long executorId, int atomicInt) {
        if (atomicInt < 0) {
            throw new RuntimeException("Block size is out of scope which is " + Integer.MAX_VALUE);
        }
        return (executorId << 32) + atomicInt;
    }

    public static int getAtomicInteger() {
        return ATOMIC_INT.getAndIncrement();
    }

    // transform [startPartition, endPartition] -> [server1, server2] to
    // {partition1 -> [server1, server2], partition2 - > [server1, server2]}
    public static Map<Integer, List<ShuffleServerInfo>> getPartitionToServers(
            RssProtos.GetShuffleAssignmentsResponse response) {
        Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
        List<PartitionRangeAssignment> assigns = response.getAssignmentsList();
        for (PartitionRangeAssignment partitionRangeAssignment: assigns) {
            final int startPartition = partitionRangeAssignment.getStartPartition();
            final int endPartition = partitionRangeAssignment.getEndPartition();
            final List<ShuffleServerInfo> shuffleServerInfos = partitionRangeAssignment
                    .getServerList()
                    .parallelStream()
                    .map(ss -> new ShuffleServerInfo(ss.getId(), ss.getIp(), ss.getPort()))
                    .collect(Collectors.toList());
            for (int i = startPartition; i <= endPartition; i++) {
                partitionToServers.put(i, shuffleServerInfos);
            }
        }
        if (partitionToServers.isEmpty()) {
            throw new RuntimeException("Empty assignment to Shuffle Server");
        }
        return partitionToServers;
    }

    // get all ShuffleRegisterInfo with [shuffleServer, startPartitionId, endPartitionId]
    public static List<ShuffleRegisterInfo> getShuffleRegisterInfos(
            RssProtos.GetShuffleAssignmentsResponse response) {
        // make the list thread safe, or get incorrect result in parallelStream
        List<ShuffleRegisterInfo> shuffleRegisterInfos = Collections.synchronizedList(Lists.newArrayList());
        List<PartitionRangeAssignment> assigns = response.getAssignmentsList();
        for (PartitionRangeAssignment assign : assigns) {
            List<ShuffleServerId> shuffleServerIds = assign.getServerList();
            final int startPartition = assign.getStartPartition();
            final int endPartition = assign.getEndPartition();
            if (shuffleServerIds != null) {
                shuffleServerIds.parallelStream().forEach(ssi -> {
                            ShuffleServerInfo shuffleServerInfo =
                                    new ShuffleServerInfo(ssi.getId(), ssi.getIp(), ssi.getPort());
                            ShuffleRegisterInfo shuffleRegisterInfo = new ShuffleRegisterInfo(shuffleServerInfo,
                                    startPartition, endPartition);
                            shuffleRegisterInfos.add(shuffleRegisterInfo);
                        }
                );
            }
        }
        return shuffleRegisterInfos;
    }

    public static String transBlockIdsToJson(Map<Integer, Set<Long>> partitionToBlockIds)
            throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(partitionToBlockIds);
    }

    public static Map<Integer, Set<Long>> getBlockIdsFromJson(String jsonStr) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<Integer, Set<Long>>> typeRef
                = new TypeReference<HashMap<Integer, Set<Long>>>() {
        };
        return mapper.readValue(jsonStr, typeRef);
    }
}
