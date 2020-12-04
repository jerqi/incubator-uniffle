package com.tencent.rss.coordinator.assignment;

import com.tencent.rss.coordinator.metadata.PartitionRange;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Class that represent the assignment of different partition ranges
 */
public class PartitionRangeAssignment {
    private final String appId;
    private final int shuffleId;
    private final Map<PartitionRange, Set<ShuffleServerId>> assignments;

    public PartitionRangeAssignment(String appId, int shuffleId,
            Map<PartitionRange, Set<ShuffleServerId>> assignments) {
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.assignments = assignments;
    }

    public String getAppId() {
        return appId;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    public Map<PartitionRange, Set<ShuffleServerId>> getAssignments() {
        return assignments;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionRangeAssignment that = (PartitionRangeAssignment) o;
        return shuffleId == that.shuffleId
                && Objects.equals(appId, that.appId)
                && Objects.equals(assignments, that.assignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(appId, shuffleId, assignments);
    }

    public static GetShuffleAssignmentsResponse convert(PartitionRangeAssignment prAssignment) {
//        List<ShuffleServerIdWithPartitionInfo> shuffleServerIdWithPartitionInfos = new ArrayList<>();
//        for (Entry<PartitionRange, Set<ShuffleServerId>> entry: prAssignment.getAssignments().entrySet()) {
//
//            shuffleServerIdWithPartitionInfos.add(shuffleServerIdWithPartitionInfo);
//        }
//        return GetShuffleAssignmentsResponse
//                .newBuilder()
//                .addAllServerInfos(shuffleServerIdWithPartitionInfos)
//                .build();
        return null;
    }
}
