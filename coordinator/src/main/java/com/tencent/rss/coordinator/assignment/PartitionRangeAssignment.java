package com.tencent.rss.coordinator.assignment;

import com.tencent.rss.coordinator.metadata.PartitionRange;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;

/**
 * Class that represents the assignment of different partition ranges
 */
public class PartitionRangeAssignment {
    private final String appId;
    private final int shuffleId;
    private final SortedMap<PartitionRange, Set<ShuffleServerId>> assignments;
    private final Map<ShuffleServerId, Set<PartitionRange>> reversedAssignments;

    public PartitionRangeAssignment(String appId, int shuffleId,
            SortedMap<PartitionRange, Set<ShuffleServerId>> assignments,
            Map<ShuffleServerId, Set<PartitionRange>> reversedAssignments) {
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.assignments = assignments;
        this.reversedAssignments = reversedAssignments;
    }

    public String getAppId() {
        return appId;
    }

    public int getShuffleId() {
        return shuffleId;
    }

    public SortedMap<PartitionRange, Set<ShuffleServerId>> getAssignments() {
        return assignments;
    }

    public Map<ShuffleServerId, Set<PartitionRange>> getReversedAssignments() {
        return reversedAssignments;
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

    public static GetShuffleAssignmentsResponse convert(PartitionRangeAssignment pra) {
        List<RssProtos.PartitionRangeAssignment> praList = new ArrayList<>();
        for (Entry<PartitionRange, Set<ShuffleServerId>> entry: pra.getAssignments().entrySet()) {
            final int start = entry.getKey().getStart();
            final int end = entry.getKey().getEnd();
            final RssProtos.PartitionRangeAssignment partitionRangeAssignment =
                    RssProtos.PartitionRangeAssignment
                    .newBuilder()
                    .setStartPartition(start)
                    .setEndPartition(end)
                    .addAllServer(entry.getValue())
                    .build();
            praList.add(partitionRangeAssignment);
        }
        return GetShuffleAssignmentsResponse.newBuilder().addAllAssignments(praList).build();
    }
}
