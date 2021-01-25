package com.tencent.rss.test.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.PartitionRangeAssignment;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.test.IntegrationTestBase;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CoordinatorGrpcClientTest extends IntegrationTestBase {

  private CoordinatorClientFactory factory = new CoordinatorClientFactory("GRPC");
  private CoordinatorGrpcClient coordinatorClient;

  @Before
  public void createClient() {
    coordinatorClient =
        (CoordinatorGrpcClient) factory.createCoordinatorClient(LOCALHOST, COORDINATOR_PORT);
  }

  @After
  public void closeClient() {
    if (coordinatorClient != null) {
      coordinatorClient.close();
    }
  }

  @Test
  public void testGetPartitionToServers() {

    GetShuffleAssignmentsResponse testResponse = generateShuffleAssignmentsResponse();

    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        coordinatorClient.getPartitionToServers(testResponse);

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
    List<ShuffleRegisterInfo> shuffleRegisterInfos = coordinatorClient.getShuffleRegisterInfoList(testResponse);
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
