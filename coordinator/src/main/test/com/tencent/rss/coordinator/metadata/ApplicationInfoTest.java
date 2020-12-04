package com.tencent.rss.coordinator.metadata;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ApplicationInfoTest {
    @Test
    public void testApplicationInfo() {
        ApplicationInfo applicationInfo = ApplicationInfo.build("application-000");
        assertEquals("application-000", applicationInfo.getAppId());
        assertEquals(0, applicationInfo.countShuffles());

        ShuffleInfo shuffleInfoA = ShuffleInfo.build(0);
        shuffleInfoA.addPartitionRange(PartitionRange.build(0, 5));
        shuffleInfoA.addPartitionRange(PartitionRange.build(6, 8));
        applicationInfo.addShuffleInfo(shuffleInfoA);
        assertEquals(1, applicationInfo.countShuffles());
        assertEquals(9, applicationInfo.countPartitions());
        assertEquals(2, applicationInfo.countPartitionRanges());

        ShuffleInfo shuffleInfoB = ShuffleInfo.build(1);
        shuffleInfoB.addPartitionRange(PartitionRange.build(2, 4));
        shuffleInfoB.addPartitionRange(PartitionRange.build(8, 12));
        shuffleInfoB.addPartitionRange(PartitionRange.build(122, 150));
        applicationInfo.addShuffleInfo(shuffleInfoB);
        assertEquals(2, applicationInfo.countShuffles());
        assertEquals(46, applicationInfo.countPartitions());
        assertEquals(5, applicationInfo.countPartitionRanges());
    }

}
