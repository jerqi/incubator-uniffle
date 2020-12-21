package com.tencent.rss.coordinator.assignment.strategy;

import com.tencent.rss.coordinator.assignment.PartitionRangeAssignment;
import com.tencent.rss.coordinator.metadata.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.Map;

/**
 * Interface that split partition ranges and assign them to different shuffle servers
 */
public interface PartitionRangeAssignmentStrategy {

    /**
     * Assign partition ranges to different shuffle servers
     * @param shuffleServerInfoMap current status of shuffle servers
     * @param appId application id
     * @param shuffleId shuffle id
     * @param partitionNum total partition numbers in shuffle
     * @param partitionPerServer partition numbers in each shuffle server
     * @param replica replication number
     * @return the assignment result
     */
    PartitionRangeAssignment assign(
            Map<ShuffleServerId, ShuffleServerInfo> shuffleServerInfoMap,
            String appId,
            int shuffleId,
            int partitionNum,
            int partitionPerServer,
            int replica);

    /**
     * Assign partition ranges and also update shuffle server status
     * @param shuffleServerInfoMap current status of shuffle servers
     * @param appId application id
     * @param shuffleId shuffle id
     * @param partitionNum total partition numbers in shuffle
     * @param partitionPerServer partition numbers in each shuffle server
     * @param replica replication number
     * @return the assignment result
     */
    PartitionRangeAssignment assignWithUpdate(
            Map<ShuffleServerId, ShuffleServerInfo> shuffleServerInfoMap,
            String appId,
            int shuffleId,
            int partitionNum,
            int partitionPerServer,
            int replica);
}
