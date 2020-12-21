package com.tencent.rss.coordinator.metadata;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Class to maintain metadata for partition ranges in specific shuffle
 */
public class ShuffleInfo {
    private final int shuffleId;
    private final Set<PartitionRange> partitionRangeSet;

    private ShuffleInfo(int shuffleId, Set<PartitionRange> partitionRangeSet) {
        this.shuffleId = shuffleId;
        this.partitionRangeSet = partitionRangeSet;
    }

    public static ShuffleInfo build(int shuffleId) {
        return new ShuffleInfo(shuffleId, new HashSet<>());
    }

    public static ShuffleInfo build(int shuffleId, Set<PartitionRange> partitionRangeSet) {
        return new ShuffleInfo(shuffleId, partitionRangeSet);
    }

    public int getShuffleId() {
        return shuffleId;
    }

    public Set<PartitionRange> getPartitionRangeSet() {
        return partitionRangeSet;
    }

    public int countPartitionRanges() {
        return partitionRangeSet.size();
    }

    public int countPartitions() {
        int total = 0;
        for (PartitionRange pr: partitionRangeSet) {
            total = total + pr.getPartitionNum();
        }
        return total;
    }

    public void addPartitionRange(PartitionRange partitionRange) {
        partitionRangeSet.add(partitionRange);
    }

    public void addAllPartitionRanges(Set<PartitionRange> partitionRanges) {
        partitionRangeSet.addAll(partitionRanges);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShuffleInfo that = (ShuffleInfo) o;
        return shuffleId == that.shuffleId
                && Objects.equals(partitionRangeSet, that.partitionRangeSet);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shuffleId, partitionRangeSet);
    }

    @Override
    public String toString() {
        return "ShuffleInfo{" + "shuffleId=" + shuffleId
                + ", partitionRangeSet=" + partitionRangeSet + '}';
    }
}
