package com.tencent.rss.coordinator.metadata;

import static org.junit.Assert.assertEquals;

import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import org.junit.Test;

public class ShuffleServerInfoTest {
    @Test
    public void testShuffleServerInfo() {
        ShuffleServerId shuffleServerId = ShuffleServerId
                .newBuilder()
                .setId("server1")
                .setIp("127.0.0.1")
                .setPort(1000)
                .build();
        ShuffleServerInfo shuffleServerInfo = ShuffleServerInfo.build(shuffleServerId);

        ApplicationInfo applicationInfo1 = ApplicationInfo.build("application-000");

        ShuffleInfo shuffleInfoA = ShuffleInfo.build(0);
        shuffleInfoA.addPartitionRange(PartitionRange.build(0, 5));
        shuffleInfoA.addPartitionRange(PartitionRange.build(6, 8));
        applicationInfo1.addShuffleInfo(shuffleInfoA);

        ShuffleInfo shuffleInfoB = ShuffleInfo.build(1);
        shuffleInfoB.addPartitionRange(PartitionRange.build(2, 4));
        shuffleInfoB.addPartitionRange(PartitionRange.build(8, 12));
        shuffleInfoB.addPartitionRange(PartitionRange.build(122, 150));
        applicationInfo1.addShuffleInfo(shuffleInfoB);

        shuffleServerInfo.addApplicationInfo(applicationInfo1);

        ApplicationInfo applicationInfo2 = ApplicationInfo.build("application-111");
        ShuffleInfo shuffleInfoC = ShuffleInfo.build(0);
        shuffleInfoC.addPartitionRange(PartitionRange.build(0, 1));
        applicationInfo2.addShuffleInfo(shuffleInfoC);

        shuffleServerInfo.addApplicationInfo(applicationInfo2);

        assertEquals(2, shuffleServerInfo.countApplications());
        assertEquals(3, shuffleServerInfo.countShuffles());
        assertEquals(48, shuffleServerInfo.countPartitions());
        assertEquals(6, shuffleServerInfo.countPartitionRanges());
    }

}
