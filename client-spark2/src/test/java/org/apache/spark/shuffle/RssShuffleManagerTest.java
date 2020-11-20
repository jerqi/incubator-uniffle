package org.apache.spark.shuffle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.tencent.rss.common.CoordinatorGrpcClient;
import com.tencent.rss.common.ShuffleServerHandler;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class RssShuffleManagerTest {

    private static RssShuffleManager MANAGER;

    @BeforeAll
    public static void init() {
        SparkConf conf = new SparkConf();
        conf.setAppName("testApp").setMaster("local[2]")
                .set("spark.rss.coordinator.ip", "0.0.0.0")
                .set("spark.rss.coordinator.port", "100");
        SparkContext sc = new SparkContext(conf);
        MANAGER = new RssShuffleManager(conf);
    }

    @Test
    public void registerShuffleTest() {
        CoordinatorGrpcClient clientMock = mock(CoordinatorGrpcClient.class);
        RssShuffleManager managerSpy = spy(MANAGER);
        doReturn(clientMock).when(managerSpy).getCoordinatorClient();
        doNothing().when(managerSpy).registerShuffleServers(anyString(), anyInt(), any());
        when(clientMock.getShuffleAssignments(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(
                getMockAssignmentsResponse());
        ShuffleHandle handle = managerSpy.registerShuffle(1, 100, null);
        assertTrue(handle instanceof RssShuffleHandle);
        ShuffleServerHandler ssh = ((RssShuffleHandle) handle).getShuffleServerHandler();
        assertEquals(1, ((RssShuffleHandle) handle).getShuffleId());
        assertEquals(100, ((RssShuffleHandle) handle).getNumMaps());
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
        assertNull(ssh.getShuffleServers(4));
    }

    private RssProtos.GetShuffleAssignmentsResponse getMockAssignmentsResponse() {
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

        return RssProtos.GetShuffleAssignmentsResponse.newBuilder()
                .addAllServerInfos(Arrays.asList(assignment1, assignment2))
                .build();

    }
}
