package com.tencent.rss.test.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
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
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
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
