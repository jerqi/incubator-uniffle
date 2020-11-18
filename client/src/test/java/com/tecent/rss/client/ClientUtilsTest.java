package com.tecent.rss.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tencent.rss.common.ShuffleServerHandler;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class ClientUtilsTest {

    @Test
    public void getBlockIdTest() {
        assertTrue(
                9223372034707292159L == ClientUtils.getBlockId(Integer.MAX_VALUE, Integer.MAX_VALUE));
        assertTrue(0L == ClientUtils.getBlockId(0, 0));
        assertTrue(2147483647L == ClientUtils.getBlockId(0, Integer.MAX_VALUE));
        assertTrue(6442450943L == ClientUtils.getBlockId(1, Integer.MAX_VALUE));
        assertTrue(9223372032559808513L == ClientUtils.getBlockId(Integer.MAX_VALUE, 1));
        assertTrue(5299989644498L == ClientUtils.getBlockId(1234, 1234));
    }

    @Test
    public void getAtomicIntegerTest() {
        int atomicId = ClientUtils.getAtomicInteger();
        assertTrue((atomicId + 1) == ClientUtils.getAtomicInteger());
    }

    @Test
    public void toShuffleServerHandlerTest() {
        RssProtos.ShuffleServerId ss1 = RssProtos.ShuffleServerId.newBuilder()
                .setIp("0.0.0.1")
                .setPort(100)
                .setId("id1")
                .build();

        RssProtos.ShuffleServerId ss2 = RssProtos.ShuffleServerId.newBuilder()
                .setIp("0.0.0.2")
                .setPort(100)
                .setId("id2")
                .build();

        RssProtos.ShuffleServerId ss3 = RssProtos.ShuffleServerId.newBuilder()
                .setIp("0.0.0.3")
                .setPort(100)
                .setId("id3")
                .build();

        RssProtos.ShuffleServerId ss4 = RssProtos.ShuffleServerId.newBuilder()
                .setIp("0.0.0.4")
                .setPort(100)
                .setId("id4")
                .build();

        RssProtos.ShuffleServerIdWithPartitionInfo assignment1 =
                RssProtos.ShuffleServerIdWithPartitionInfo.newBuilder()
                        .addAllPartitions(Arrays.asList(0, 1))
                        .addAllServer(Arrays.asList(ss1, ss2))
                        .build();

        RssProtos.ShuffleServerIdWithPartitionInfo assignment2 =
                RssProtos.ShuffleServerIdWithPartitionInfo.newBuilder()
                        .addAllPartitions(Arrays.asList(2, 3))
                        .addAllServer(Arrays.asList(ss3, ss4))
                        .build();

        RssProtos.GetShuffleAssignmentsResponse testResponse =
                RssProtos.GetShuffleAssignmentsResponse.newBuilder()
                        .addAllServerInfos(Arrays.asList(assignment1, assignment2))
                        .build();

        ShuffleServerHandler ssh = ClientUtils.toShuffleServerHandler(testResponse);

        assertEquals(Arrays.asList(new ShuffleServerInfo("id1", "0.0.0.1", 100),
                new ShuffleServerInfo("id2", "0.0.0.2", 100)),
                ssh.getShuffleServers(0));
        assertEquals(Arrays.asList(new ShuffleServerInfo("id1", "0.0.0.1", 100),
                new ShuffleServerInfo("id2", "0.0.0.2", 100)),
                ssh.getShuffleServers(1));
        assertEquals(Arrays.asList(new ShuffleServerInfo("id3", "0.0.0.3", 100),
                new ShuffleServerInfo("id4", "0.0.0.4", 100)),
                ssh.getShuffleServers(2));
        assertEquals(Arrays.asList(new ShuffleServerInfo("id3", "0.0.0.3", 100),
                new ShuffleServerInfo("id4", "0.0.0.4", 100)),
                ssh.getShuffleServers(3));
        assertEquals(null, ssh.getShuffleServers(4));
    }
}
