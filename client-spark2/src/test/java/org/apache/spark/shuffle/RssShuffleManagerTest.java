package org.apache.spark.shuffle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.tencent.rss.common.CoordinatorGrpcClient;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.PartitionRangeAssignment;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RssShuffleManagerTest {

  private static RssShuffleManager MANAGER;

  @BeforeClass
  public static void init() {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp").setMaster("local[2]")
        .set("spark.rss.coordinator.ip", "0.0.0.0")
        .set("spark.rss.coordinator.port", "100");
    // init SparkContext
    SparkContext sc = SparkContext.getOrCreate(conf);
    MANAGER = new RssShuffleManager(conf, true);
  }

  @AfterClass
  public static void stop() {
    SparkContext.getOrCreate().stop();
  }

  @Test
  public void registerShuffleTest() {
    CoordinatorGrpcClient clientMock = mock(CoordinatorGrpcClient.class);
    RssShuffleManager managerSpy = spy(MANAGER);
    doReturn(clientMock).when(managerSpy).getCoordinatorClient();
    doNothing().when(managerSpy).registerShuffleServers(any(), anyInt(), any());
    doNothing().when(managerSpy).setAppId();
    when(clientMock.getShuffleAssignments(any(), anyInt(), anyInt(), anyInt())).thenReturn(
        getMockAssignmentsResponse());
    ShuffleDependency dependencyMock = mock(ShuffleDependency.class);
    Partitioner partitionerMock = mock(Partitioner.class);
    when(dependencyMock.partitioner()).thenReturn(partitionerMock);
    when(partitionerMock.numPartitions()).thenReturn(0);

    ShuffleHandle handle = managerSpy.registerShuffle(1, 100, dependencyMock);
    assertTrue(handle instanceof RssShuffleHandle);
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        ((RssShuffleHandle) handle).getPartitionToServers();
    assertEquals(1, ((RssShuffleHandle) handle).getShuffleId());
    assertEquals(100, ((RssShuffleHandle) handle).getNumMaps());
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
