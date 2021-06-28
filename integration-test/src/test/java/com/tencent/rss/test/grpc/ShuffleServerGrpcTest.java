package com.tencent.rss.test.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
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
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 2000L);
    shuffleServerConf.setLong("rss.server.preAllocation.expired", 5000L);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @Test
  public void clearResourceTest() throws Exception {
    final ShuffleWriteClient shuffleWriteClient =
        ShuffleClientFactory.getInstance().createShuffleWriteClient(
            "GRPC", 2, 10000L, 4);
    shuffleWriteClient.registerCoordinators("127.0.0.1:19999");
    shuffleWriteClient.registerShuffle(
        new ShuffleServerInfo("127.0.0.1-20001", "127.0.0.1", 20001),
        "clearResourceTest1",
        0,
        0,
        1);

    shuffleWriteClient.sendAppHeartbeat("clearResourceTest1", 1000L);
    shuffleWriteClient.sendAppHeartbeat("clearResourceTest2", 1000L);

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
        shuffleWriteClient.sendAppHeartbeat("clearResourceTest1", 1000L);
        i++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          return;
        }
      }
    });
    t.start();

    // Heartbeat is sent to coordinator too]
    Thread.sleep(3000);
    shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest("clearResourceTest1", 0, 0, 1));
    assertEquals(Sets.newHashSet("clearResourceTest1"),
        coordinators.get(0).getApplicationManager().getAppIds());
    // clearResourceTest2 will be removed because of rss.server.app.expired.withoutHeartbeat
    Thread.sleep(2000);
    assertEquals(Sets.newHashSet("clearResourceTest1"),
        shuffleServers.get(0).getShuffleTaskManager().getAppIds().keySet());

    // clearResourceTest1 will be removed because of rss.server.app.expired.withoutHeartbeat
    t.interrupt();
    Thread.sleep(8000);
    assertEquals(0, shuffleServers.get(0).getShuffleTaskManager().getAppIds().size());

  }

  @Test
  public void shuffleResultTest() throws Exception {
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Lists.newArrayList(1L, 2L, 3L));
    partitionToBlockIds.put(2, Lists.newArrayList(4L, 5L));
    partitionToBlockIds.put(3, Lists.newArrayList(6L));

    RssReportShuffleResultRequest request =
        new RssReportShuffleResultRequest("appId", 0, 1L, partitionToBlockIds);
    try {
      shuffleServerClient.reportShuffleResult(request);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("error happened when report shuffle result"));
    }

    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("appId", 100, 0, 1);
    shuffleServerClient.registerShuffle(rrsr);

    RssReportShuffleResultResponse response = shuffleServerClient.reportShuffleResult(request);
    assertEquals(ResponseStatusCode.SUCCESS, response.getStatusCode());
    partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Lists.newArrayList(11L, 12L, 13L));
    partitionToBlockIds.put(2, Lists.newArrayList(14L, 15L));
    partitionToBlockIds.put(3, Lists.newArrayList(16L));

    request =
        new RssReportShuffleResultRequest("appId", 0, 2L, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(request);
    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest("appId", 0, 1, Lists.newArrayList(1L));
    RssGetShuffleResultResponse result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(1L, 2L, 3L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 2, Lists.newArrayList(1L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(4L, 5L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 3, Lists.newArrayList(1L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(6L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 1, Lists.newArrayList(2L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(11L, 12L, 13L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 2, Lists.newArrayList(2L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(14L, 15L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 3, Lists.newArrayList(2L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(16L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 1, Lists.newArrayList(1L, 2L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(1L, 2L, 3L, 11L, 12L, 13L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 2, Lists.newArrayList(1L, 2L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(4L, 5L, 14L, 15L), result.getBlockIds());

    req = new RssGetShuffleResultRequest("appId", 0, 3, Lists.newArrayList(1L, 2L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(6L, 16L), result.getBlockIds());

    partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Lists.newArrayList(7L, 8L));
    request = new RssReportShuffleResultRequest("appId", 0, 3L, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(request);

    req = new RssGetShuffleResultRequest("appId", 0, 1, Lists.newArrayList(1L, 2L, 3L));
    result = shuffleServerClient.getShuffleResult(req);
    assertEquals(Lists.newArrayList(1L, 2L, 3L, 11L, 12L, 13L, 7L, 8L), result.getBlockIds());

    request =
        new RssReportShuffleResultRequest("appId", 1, 1L, Maps.newHashMap());
    shuffleServerClient.reportShuffleResult(request);
    req = new RssGetShuffleResultRequest("appId", 1, 1, Lists.newArrayList(1L, 2L));
    result = shuffleServerClient.getShuffleResult(req);
    assertTrue(result.getBlockIds().isEmpty());

    // wait resources are deleted
    Thread.sleep(12000);
    req = new RssGetShuffleResultRequest("appId", 1, 1, Lists.newArrayList(1L, 2L));
    try {
      shuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("can't find shuffle data"));
    }

    // doesn't registered
    req = new RssGetShuffleResultRequest("appId1", 1, 1, Lists.newArrayList(1L, 2L));
    try {
      shuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("can't find shuffle data"));
    }
    rrsr = new RssRegisterShuffleRequest("appId1", 100, 0, 1);
    shuffleServerClient.registerShuffle(rrsr);
    req = new RssGetShuffleResultRequest("appId1", 1, 1, Lists.newArrayList());
    result = shuffleServerClient.getShuffleResult(req);
    assertTrue(result.getBlockIds().isEmpty());
  }

  @Test
  public void noRegisterTest() throws Exception {
    List<ShuffleBlockInfo> blockInfos = Lists.newArrayList(new ShuffleBlockInfo(0, 0, 0, 100, 0,
        new byte[]{}, Lists.newArrayList(), 0));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest("appId", 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    assertEquals(100, shuffleServers.get(0).getPreAllocatedMemory());
    Thread.sleep(10000);
    assertEquals(0, shuffleServers.get(0).getPreAllocatedMemory());
  }

  @Test
  public void multipleShuffleResultTest() throws Exception {
    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("appId", 100, 0, 1);
    shuffleServerClient.registerShuffle(rrsr);

    Runnable r1 = () -> {
      for (long i = 0; i < 100; i++) {
        Map<Integer, List<Long>> ptbs = Maps.newHashMap();
        List<Long> blockIds = Lists.newArrayList();
        blockIds.add(i);
        ptbs.put(1, blockIds);
        RssReportShuffleResultRequest req1 =
            new RssReportShuffleResultRequest("appId", 1, i, ptbs);
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
            new RssReportShuffleResultRequest("appId", 1, i, ptbs);
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
            new RssReportShuffleResultRequest("appId", 1, i, ptbs);
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
    List<Long> taskAttemptIds = Lists.newArrayList();
    for (long i = 0; i < 300; i++) {
      expectedBlockIds.add(i);
      taskAttemptIds.add(i);
    }

    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest(
        "appId", 1, 1, taskAttemptIds);
    RssGetShuffleResultResponse result = shuffleServerClient.getShuffleResult(req);
    assertEquals(expectedBlockIds.size(), result.getBlockIds().size());
    assertTrue(expectedBlockIds.containsAll(result.getBlockIds()));
  }
}
