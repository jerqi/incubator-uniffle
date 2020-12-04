package com.tencent.rss.coordinator.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

public class ShuffleInfoTest {
    @Test
    public void testShuffleInfo() {
        ShuffleInfo shuffleInfoA = ShuffleInfo.build(0);
        shuffleInfoA.addPartitionRange(PartitionRange.build(0, 5));
        shuffleInfoA.addPartitionRange(PartitionRange.build(6, 8));
        assertEquals(9, shuffleInfoA.countPartitions());
        assertEquals(2, shuffleInfoA.countPartitionRanges());

        ShuffleInfo shuffleInfoB = ShuffleInfo.build(1);
        shuffleInfoB.addPartitionRange(PartitionRange.build(0, 5));
        shuffleInfoB.addPartitionRange(PartitionRange.build(6, 8));
        assertNotEquals(shuffleInfoA, shuffleInfoB);
    }

}
