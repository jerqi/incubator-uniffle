package com.tencent.rss.test.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.test.IntegrationTestBase;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShuffleServerGrpcTest extends IntegrationTestBase {

  private ShuffleServerGrpcClient shuffleServerClient;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 2000);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.app.expired.withHeartbeat", 10000L);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 5000L);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @Test
  public void clearResourceTest() throws Exception {
    CoordinatorClientFactory coordinatorFactory = new CoordinatorClientFactory("GRPC");
    CoordinatorGrpcClient coordinatorClient =
        (CoordinatorGrpcClient) coordinatorFactory.createCoordinatorClient(LOCALHOST, COORDINATOR_PORT);
    coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("clearResourceTest1"));
    coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("clearResourceTest2"));

    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("clearResourceTest1", 0, 0, 1);
    shuffleServerClient.registerShuffle(rrsr);
    rrsr = new RssRegisterShuffleRequest("clearResourceTest2", 0, 0, 1);
    shuffleServerClient.registerShuffle(rrsr);
    assertEquals(Sets.newHashSet("clearResourceTest1", "clearResourceTest2"),
        shuffleServers.get(0).getShuffleTaskManager().getAppIds().keySet());
    // Thread will keep refresh clearResourceTest1 in coordinator
    Thread t = new Thread(() -> {
      int i = 0;
      while (i < 20) {
        coordinatorClient.sendAppHeartBeat(new RssAppHeartBeatRequest("clearResourceTest1"));
        i++;
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
        }
      }
    });
    t.start();

    // after 3s, coordinator will remove expired appId,
    // but shuffle server won't because of rss.server.app.expired.withoutHeartbeat
    Thread.sleep(3000);
    shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest("clearResourceTest1", 0, 0, 1));
    assertEquals(Sets.newHashSet("clearResourceTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
    assertEquals(Sets.newHashSet("clearResourceTest1", "clearResourceTest2"),
        shuffleServers.get(0).getShuffleTaskManager().getAppIds().keySet());
    // clearResourceTest2 will be removed because of rss.server.app.expired.withoutHeartbeat
    Thread.sleep(4000);
    assertEquals(Sets.newHashSet("clearResourceTest1"),
        shuffleServers.get(0).getShuffleTaskManager().getAppIds().keySet());
    // clearResourceTest1 will be removed because of rss.server.app.expired.withHeartbeat
    Thread.sleep(8000);
    assertEquals(Sets.newHashSet("clearResourceTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
    assertEquals(0, shuffleServers.get(0).getShuffleTaskManager().getAppIds().size());
    t.join(10000);
  }

  @Test
  public void shuffleResultTest() {
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Lists.newArrayList(1L, 2L, 3L));
    partitionToBlockIds.put(2, Lists.newArrayList(4L, 5L));
    partitionToBlockIds.put(3, Lists.newArrayList(6L));
    RssReportShuffleResultRequest request =
        new RssReportShuffleResultRequest("appId", 0, partitionToBlockIds);
    RssReportShuffleResultResponse response = shuffleServerClient.reportShuffleResult(request);
    assertEquals(ResponseStatusCode.SUCCESS, response.getStatusCode());
    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest("appId", 0, 1);
    RssGetShuffleResultResponse result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(1L, 2L, 3L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 2);
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(4L, 5L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 3);
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(6L), result.getBlockIds());

    partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Lists.newArrayList(7L, 8L));
    request = new RssReportShuffleResultRequest("appId", 0, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(request);

    req = new RssGetShuffleResultRequest("appId", 0, 1);
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(1L, 2L, 3L, 7L, 8L), result.getBlockIds());
  }

  @Test
  public void multipleShuffleResultTest() throws Exception {
    Runnable r1 = () -> {
      for (long i = 0; i < 100; i++) {
        Map<Integer, List<Long>> ptbs = Maps.newHashMap();
        List<Long> blockIds = Lists.newArrayList();
        blockIds.add(i);
        ptbs.put(1, blockIds);
        RssReportShuffleResultRequest req1 =
            new RssReportShuffleResultRequest("appId", 1, ptbs);
        shuffleServerClient.reportShuffleResult(req1);
      }
    };
    Runnable r2 = () -> {
      for (long i = 100; i < 200; i++) {
        Map<Integer, List<Long>> ptbs = Maps.newHashMap();
        List<Long> blockIds = Lists.newArrayList();
        blockIds.add(i);
        ptbs.put(1, blockIds);
        RssReportShuffleResultRequest req1 =
            new RssReportShuffleResultRequest("appId", 1, ptbs);
        shuffleServerClient.reportShuffleResult(req1);
      }
    };
    Runnable r3 = () -> {
      for (long i = 200; i < 300; i++) {
        Map<Integer, List<Long>> ptbs = Maps.newHashMap();
        List<Long> blockIds = Lists.newArrayList();
        blockIds.add(i);
        ptbs.put(1, blockIds);
        RssReportShuffleResultRequest req1 =
            new RssReportShuffleResultRequest("appId", 1, ptbs);
        shuffleServerClient.reportShuffleResult(req1);
      }
    };
    Thread t1 = new Thread(r1);
    Thread t2 = new Thread(r2);
    Thread t3 = new Thread(r3);
    t1.start();
    t2.start();
    t3.start();
    t1.join();
    t2.join();
    t3.join();

    List<Long> expectedBlockIds = Lists.newArrayList();
    for (long i = 0; i < 300; i++) {
      expectedBlockIds.add(i);
    }

    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest("appId", 1, 1);
    RssGetShuffleResultResponse result = shuffleServerClient.getShuffleResult(req);
    assertEquals(expectedBlockIds.size(), result.getBlockIds().size());
    assertTrue(expectedBlockIds.containsAll(result.getBlockIds()));
  }
}
