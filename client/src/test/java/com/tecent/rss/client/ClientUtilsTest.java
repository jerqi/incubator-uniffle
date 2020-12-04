package com.tecent.rss.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.PartitionRangeAssignment;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

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
        GetShuffleAssignmentsResponse testResponse = generateShuffleAssignmentsResponse();

        Map<Integer, List<ShuffleServerInfo>> partitionToServers =
                ClientUtils.getPartitionToServers(testResponse);

        assertEquals(Arrays.asList(new ShuffleServerInfo("id1", "0.0.0.1", 100),
                new ShuffleServerInfo("id2", "0.0.0.2", 100)),
                partitionToServers.get(0));
        assertEquals(Arrays.asList(new ShuffleServerInfo("id1", "0.0.0.1", 100),
                new ShuffleServerInfo("id2", "0.0.0.2", 100)),
                partitionToServers.get(1));
        assertEquals(Arrays.asList(new ShuffleServerInfo("id3", "0.0.0.3", 100),
                new ShuffleServerInfo("id4", "0.0.0.4", 100)),
                partitionToServers.get(2));
        assertEquals(Arrays.asList(new ShuffleServerInfo("id3", "0.0.0.3", 100),
                new ShuffleServerInfo("id4", "0.0.0.4", 100)),
                partitionToServers.get(3));
        assertNull(partitionToServers.get(4));
    }

    @Test
    public void getShuffleRegisterInfosTest() {
        GetShuffleAssignmentsResponse testResponse = generateShuffleAssignmentsResponse();
        List<ShuffleRegisterInfo> shuffleRegisterInfos = ClientUtils.getShuffleRegisterInfos(testResponse);
        List<ShuffleRegisterInfo> expected = Arrays.asList(
                new ShuffleRegisterInfo(new ShuffleServerInfo("id1", "0.0.0.1", 100), 0, 1),
                new ShuffleRegisterInfo(new ShuffleServerInfo("id2", "0.0.0.2", 100), 0, 1),
                new ShuffleRegisterInfo(new ShuffleServerInfo("id3", "0.0.0.3", 100), 2, 3),
                new ShuffleRegisterInfo(new ShuffleServerInfo("id4", "0.0.0.4", 100), 2, 3));
        assertEquals(4, shuffleRegisterInfos.size());
        for (ShuffleRegisterInfo sri : expected) {
            assertTrue(shuffleRegisterInfos.contains(sri));
        }
    }

    @Test
    public void transformTest() throws Exception {
        Map<Integer, Set<Long>> partitionToBlockIds = Maps.newHashMap();
        partitionToBlockIds.put(1, Sets.newHashSet(1L, 2L, 3L));
        partitionToBlockIds.put(2, Sets.newHashSet(4L, 5L, 6L));
        partitionToBlockIds.put(3, Sets.newHashSet(7L, 8L, 9L));
        String jsonStr = ClientUtils.transBlockIdsToJson(partitionToBlockIds);
        Map<Integer, Set<Long>> actualMap = ClientUtils.getBlockIdsFromJson(jsonStr);
        assertEquals(partitionToBlockIds, actualMap);
    }

    private GetShuffleAssignmentsResponse generateShuffleAssignmentsResponse() {
        ShuffleServerId ss1 = RssProtos.ShuffleServerId.newBuilder()
                .setIp("0.0.0.1")
                .setPort(100)
                .setId("id1")
                .build();

        ShuffleServerId ss2 = RssProtos.ShuffleServerId.newBuilder()
                .setIp("0.0.0.2")
                .setPort(100)
                .setId("id2")
                .build();

        ShuffleServerId ss3 = RssProtos.ShuffleServerId.newBuilder()
                .setIp("0.0.0.3")
                .setPort(100)
                .setId("id3")
                .build();

        ShuffleServerId ss4 = RssProtos.ShuffleServerId.newBuilder()
                .setIp("0.0.0.4")
                .setPort(100)
                .setId("id4")
                .build();

        PartitionRangeAssignment assignment1 =
                RssProtos.PartitionRangeAssignment.newBuilder()
                        .setStartPartition(0)
                        .setEndPartition(1)
                        .addAllServer(Arrays.asList(ss1, ss2))
                        .build();

        PartitionRangeAssignment assignment2 =
                RssProtos.PartitionRangeAssignment.newBuilder()
                        .setStartPartition(2)
                        .setEndPartition(3)
                        .addAllServer(Arrays.asList(ss3, ss4))
                        .build();

        return RssProtos.GetShuffleAssignmentsResponse.newBuilder()
                .addAllAssignments(Arrays.asList(assignment1, assignment2))
                .build();
    }
}
