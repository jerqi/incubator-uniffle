package com.tencent.rss.test.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssGetShuffleAssignmentsRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAppHeartBeatResponse;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse;
import com.tencent.rss.proto.RssProtos.PartitionRangeAssignment;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.test.IntegrationTestBase;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoordinatorGrpcTest extends IntegrationTestBase {

  private CoordinatorClientFactory factory = new CoordinatorClientFactory("GRPC");
  private CoordinatorGrpcClient coordinatorClient;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 2000);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();
  }

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
  public void getShuffleRegisterInfoTest() {
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

  @Test
  public void getShuffleAssignmentsTest() throws Exception {
    waitForRegister(1);
    RssGetShuffleAssignmentsRequest request = new RssGetShuffleAssignmentsRequest("appId", 1, 10, 4);
    RssGetShuffleAssignmentsResponse response = coordinatorClient.getShuffleAssignments(request);
    Set<Integer> expectedStart = Sets.newHashSet(0, 4, 8);

    assertEquals(3, response.getRegisterInfoList().size());
    for (ShuffleRegisterInfo sri : response.getRegisterInfoList()) {
      switch (sri.getStart()) {
        case 0:
          assertEquals(3, sri.getEnd());
          expectedStart.remove(0);
          break;
        case 4:
          assertEquals(7, sri.getEnd());
          expectedStart.remove(4);
          break;
        case 8:
          assertEquals(11, sri.getEnd());
          expectedStart.remove(8);
          break;
        default:
          fail("Shouldn't be here");
      }
    }
    assertTrue(expectedStart.isEmpty());
    assertEquals(1, response.getShuffleServersForResult().size());
  }

  @Test
  public void appHeartbeatTest() throws Exception {
    RssAppHeartBeatResponse response =
        coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("appHeartbeatTest1"));
    assertEquals(ResponseStatusCode.SUCCESS, response.getStatusCode());
    assertEquals(Sets.newHashSet("appHeartbeatTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
    coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("appHeartbeatTest2"));
    assertEquals(Sets.newHashSet("appHeartbeatTest1", "appHeartbeatTest2"),
        coordinators.get(0).getApplicationManager().getAppIds());
    int retry = 0;
    while (retry < 5) {
      coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("appHeartbeatTest1"));
      retry++;
      Thread.sleep(1000);
    }
    // appHeartbeatTest2 was removed because of expired
    assertEquals(Sets.newHashSet("appHeartbeatTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
  }

  private void waitForRegister(int expcetedServers) throws Exception {
    GetShuffleServerListResponse response;
    int count = 0;
    do {
      response = coordinatorClient.getShuffleServerList();
      Thread.sleep(1000);
      if (count > 10) {
        throw new RuntimeException("No shuffle server connected");
      }
      count++;
    } while (response.getServersCount() < expcetedServers);
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
