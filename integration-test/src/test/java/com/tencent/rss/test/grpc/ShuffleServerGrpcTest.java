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
import com.tencent.rss.common.PartitionRange;
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
import org.roaringbitmap.longlong.Roaring64NavigableMap;

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
        Lists.newArrayList(new PartitionRange(0, 1)));

    shuffleWriteClient.sendAppHeartbeat("clearResourceTest1", 1000L);
    shuffleWriteClient.sendAppHeartbeat("clearResourceTest2", 1000L);

    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("clearResourceTest1", 0,
        Lists.newArrayList(new PartitionRange(0, 1)));
    shuffleServerClient.registerShuffle(rrsr);
    rrsr = new RssRegisterShuffleRequest("clearResourceTest2", 0,
        Lists.newArrayList(new PartitionRange(0, 1)));
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
    shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest("clearResourceTest1", 0,
        Lists.newArrayList(new PartitionRange(0, 1))));
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
        new RssReportShuffleResultRequest("shuffleResultTest", 0, 0L, partitionToBlockIds);
    try {
      shuffleServerClient.reportShuffleResult(request);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("error happened when report shuffle result"));
    }

    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest("shuffleResultTest", 1, 1);
    try {
      shuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("can't find shuffle data"));
    }

    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("shuffleResultTest", 100,
        Lists.newArrayList(new PartitionRange(0, 1)));
    shuffleServerClient.registerShuffle(rrsr);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1);
    RssGetShuffleResultResponse result = shuffleServerClient.getShuffleResult(req);
    Roaring64NavigableMap blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(), blockIdBitmap);

    request =
        new RssReportShuffleResultRequest("shuffleResultTest", 0, 0L, partitionToBlockIds);
    RssReportShuffleResultResponse response = shuffleServerClient.reportShuffleResult(request);
    assertEquals(ResponseStatusCode.SUCCESS, response.getStatusCode());
    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(1L, 2L, 3L), blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 2);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(4L, 5L), blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 3);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(6L), blockIdBitmap);

    partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Lists.newArrayList(11L, 12L, 13L));
    partitionToBlockIds.put(2, Lists.newArrayList(14L, 15L));
    partitionToBlockIds.put(3, Lists.newArrayList(16L));

    request =
        new RssReportShuffleResultRequest("shuffleResultTest", 0, 1L, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(request);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 1);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(1L, 2L, 3L, 11L, 12L, 13L), blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 2);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(4L, 5L, 14L, 15L), blockIdBitmap);

    req = new RssGetShuffleResultRequest("shuffleResultTest", 0, 3);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(6L, 16L), blockIdBitmap);

    partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Lists.newArrayList(7L, 8L));
    request = new RssReportShuffleResultRequest("shuffleResultTest",
        0, 2L, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(request);

    request =
        new RssReportShuffleResultRequest("shuffleResultTest", 1, 1L, Maps.newHashMap());
    shuffleServerClient.reportShuffleResult(request);
    req = new RssGetShuffleResultRequest("shuffleResultTest", 1, 1);
    result = shuffleServerClient.getShuffleResult(req);
    blockIdBitmap = result.getBlockIdBitmap();
    assertEquals(Roaring64NavigableMap.bitmapOf(), blockIdBitmap);

    // wait resources are deleted
    Thread.sleep(12000);
    req = new RssGetShuffleResultRequest("shuffleResultTest", 1, 1);
    try {
      shuffleServerClient.getShuffleResult(req);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("can't find shuffle data"));
    }
  }

  @Test
  public void registerTest() {
    shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest("registerTest", 0,
        Lists.newArrayList(new PartitionRange(0, 1))));
    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest("registerTest", 0, 0);
    // no exception with getShuffleResult means register successfully
    shuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 0, 1);
    shuffleServerClient.getShuffleResult(req);
    shuffleServerClient.registerShuffle(new RssRegisterShuffleRequest("registerTest", 1,
        Lists.newArrayList(new PartitionRange(0, 0), new PartitionRange(1, 1), new PartitionRange(2, 2))));
    req = new RssGetShuffleResultRequest("registerTest", 1, 0);
    shuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 1, 1);
    shuffleServerClient.getShuffleResult(req);
    req = new RssGetShuffleResultRequest("registerTest", 1, 2);
    shuffleServerClient.getShuffleResult(req);
  }

  @Test
  public void sendDataWithoutRegisterTest() throws Exception {
    List<ShuffleBlockInfo> blockInfos = Lists.newArrayList(new ShuffleBlockInfo(0, 0, 0, 100, 0,
        new byte[]{}, Lists.newArrayList(), 0, 100, 0));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blockInfos);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest(
        "sendDataWithoutRegisterTest", 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    assertEquals(100, shuffleServers.get(0).getPreAllocatedMemory());
    Thread.sleep(10000);
    assertEquals(0, shuffleServers.get(0).getPreAllocatedMemory());
  }

  @Test
  public void multipleShuffleResultTest() throws Exception {
    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("multipleShuffleResultTest", 100,
        Lists.newArrayList(new PartitionRange(0, 1)));
    shuffleServerClient.registerShuffle(rrsr);

    Runnable r1 = () -> {
      for (long i = 0; i < 100; i++) {
        Map<Integer, List<Long>> ptbs = Maps.newHashMap();
        List<Long> blockIds = Lists.newArrayList();
        blockIds.add(i);
        ptbs.put(1, blockIds);
        RssReportShuffleResultRequest req1 =
            new RssReportShuffleResultRequest("multipleShuffleResultTest", 1, 0, ptbs);
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
            new RssReportShuffleResultRequest("multipleShuffleResultTest", 1, 1, ptbs);
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
            new RssReportShuffleResultRequest("multipleShuffleResultTest", 1, 2, ptbs);
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

    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    List<Long> expectedBlockIds = Lists.newArrayList();
    for (long i = 0; i < 300; i++) {
      blockIdBitmap.addLong(i);
    }

    RssGetShuffleResultRequest req = new RssGetShuffleResultRequest(
        "multipleShuffleResultTest", 1, 1);
    RssGetShuffleResultResponse result = shuffleServerClient.getShuffleResult(req);
    Roaring64NavigableMap actualBlockIdBitmap = result.getBlockIdBitmap();
    assertEquals(blockIdBitmap, actualBlockIdBitmap);
  }
}
