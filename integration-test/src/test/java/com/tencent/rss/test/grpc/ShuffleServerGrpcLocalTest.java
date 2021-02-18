package com.tencent.rss.test.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import com.tencent.rss.test.IntegrationTestBase;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShuffleServerGrpcLocalTest extends IntegrationTestBase {

  private static AtomicLong ATOMIC_LONG = new AtomicLong(0L);
  //  private ShuffleServerClientFactory factory = ShuffleServerClientFactory.getInstance();
  private ShuffleServerGrpcClient shuffleServerClient;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @After
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void writeReadTest() {
    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest("appId", 0, 0, 1);
    shuffleServerClient.registerShuffle(rrsr);
    rrsr = new RssRegisterShuffleRequest("appId", 0, 2, 3);
    shuffleServerClient.registerShuffle(rrsr);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds1 = Sets.newHashSet();
    Set<Long> expectedBlockIds2 = Sets.newHashSet();
    Set<Long> expectedBlockIds3 = Sets.newHashSet();
    Set<Long> expectedBlockIds4 = Sets.newHashSet();
    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        0, 0, 3, 25, expectedBlockIds1, expectedData);
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        0, 1, 5, 25, expectedBlockIds2, expectedData);
    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        0, 2, 4, 25, expectedBlockIds3, expectedData);
    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        0, 3, 1, 25, expectedBlockIds4, expectedData);
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    partitionToBlocks.put(1, blocks2);
    partitionToBlocks.put(2, blocks3);
    partitionToBlocks.put(3, blocks4);

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest("appId", shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    RssSendCommitRequest rscr = new RssSendCommitRequest("appId", 0);
    shuffleServerClient.sendCommit(rscr);

    RssGetShuffleDataRequest rgsdr = new RssGetShuffleDataRequest(
        "appId", 0, 0, 2, 10, 1000, expectedBlockIds1);
    ShuffleDataResult sdr = shuffleServerClient.getShuffleData(rgsdr).getShuffleDataResult();
    validateResult(sdr, expectedBlockIds1, expectedData);

    rgsdr = new RssGetShuffleDataRequest(
        "appId", 0, 1, 2, 10, 1000, expectedBlockIds2);
    sdr = shuffleServerClient.getShuffleData(rgsdr).getShuffleDataResult();
    validateResult(sdr, expectedBlockIds2, expectedData);

    rgsdr = new RssGetShuffleDataRequest(
        "appId", 0, 2, 2, 10, 1000, expectedBlockIds3);
    sdr = shuffleServerClient.getShuffleData(rgsdr).getShuffleDataResult();
    validateResult(sdr, expectedBlockIds3, expectedData);

    rgsdr = new RssGetShuffleDataRequest(
        "appId", 0, 3, 2, 10, 1000, expectedBlockIds4);
    sdr = shuffleServerClient.getShuffleData(rgsdr).getShuffleDataResult();
    validateResult(sdr, expectedBlockIds4, expectedData);
  }

  private List<ShuffleBlockInfo> createShuffleBlockList(int shuffleId, int partitionId,
      int blockNum, int length, Set<Long> blockIds, Map<Long, byte[]> dataMap) {
    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = ATOMIC_LONG.getAndIncrement();
      blockIds.add(blockId);
      dataMap.put(blockId, buf);
      shuffleBlockInfoList.add(new ShuffleBlockInfo(
          shuffleId, partitionId, blockId, length, ChecksumUtils.getCrc32(buf), buf, shuffleServerInfoList));
    }
    return shuffleBlockInfoList;
  }

  protected void validateResult(ShuffleDataResult sdr, Set<Long> expectedBlockIds,
      Map<Long, byte[]> expectedData) {
    byte[] buffer = sdr.getData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    int matched = 0;
    for (BufferSegment bs : bufferSegments) {
      if (expectedBlockIds.contains(bs.getBlockId())) {
        byte[] data = new byte[bs.getLength()];
        System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
        assertEquals(bs.getCrc(), ChecksumUtils.getCrc32(data));
        assertTrue(Arrays.equals(data, expectedData.get(bs.getBlockId())));
        assertTrue(expectedBlockIds.contains(bs.getBlockId()));
        matched++;
      }
    }
    assertEquals(expectedBlockIds.size(), matched);
  }
}
