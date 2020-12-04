package com.tencent.rss.coordinator.metadata;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PartitionRangeTest {
    @Test
    public void testPartitionRange() {
        PartitionRange partitionRangeA = PartitionRange.build(0, 3);
        PartitionRange partitionRangeB = PartitionRange.build(0, 3);
        assertEquals(4, partitionRangeA.getPartitionNum());
        assertEquals(partitionRangeA, partitionRangeB);
    }

}
