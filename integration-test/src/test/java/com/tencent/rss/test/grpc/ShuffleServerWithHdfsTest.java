package com.tencent.rss.test.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShuffleServerWithHdfsTest extends ShuffleReadWriteBase {

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

  @After
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void hdfsWriteReadTest() {
    String appId = "app_hdfs_read_write";
    String dataBasePath = HDFS_URI + "rss/test";
    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest(appId, 0, 0, 1);
    shuffleServerClient.registerShuffle(rrsr);
    rrsr = new RssRegisterShuffleRequest(appId, 0, 2, 3);
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

    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);

    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest(appId, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    RssSendCommitRequest rscr = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rscr);
    RssFinishShuffleRequest rfsr = new RssFinishShuffleRequest(appId, 0);
    try {
      // before call finishShuffle(), the data won't be flushed to hdfs
      new ShuffleReadClientImpl(StorageType.HDFS.name(),
          appId, 0, 0, 100, 2, 10, 1000, dataBasePath, expectedBlockIds1, Lists.newArrayList());
      fail("Expected exception");
    } catch (Exception e) {
      // ignore
    }
    shuffleServerClient.finishShuffle(rfsr);

    partitionToBlocks.clear();
    partitionToBlocks.put(2, blocks3);
    shuffleToBlocks.clear();
    shuffleToBlocks.put(0, partitionToBlocks);
    rssdr = new RssSendShuffleDataRequest(appId, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    rscr = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rscr);
    rfsr = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rfsr);

    partitionToBlocks.clear();
    partitionToBlocks.put(3, blocks4);
    shuffleToBlocks.clear();
    shuffleToBlocks.put(0, partitionToBlocks);
    rssdr = new RssSendShuffleDataRequest(appId, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rssdr);
    rscr = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rscr);
    rfsr = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rfsr);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        appId, 0, 0, 100, 2, 10, 1000, dataBasePath, expectedBlockIds1, Lists.newArrayList());
    validateResult(readClient, expectedData, expectedBlockIds1);

    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        appId, 0, 1, 100, 2, 10, 1000, dataBasePath, expectedBlockIds2, Lists.newArrayList());
    validateResult(readClient, expectedData, expectedBlockIds2);

    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        appId, 0, 2, 100, 2, 10, 1000, dataBasePath, expectedBlockIds3, Lists.newArrayList());
    validateResult(readClient, expectedData, expectedBlockIds3);

    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        appId, 0, 3, 100, 2, 10, 1000, dataBasePath, expectedBlockIds4, Lists.newArrayList());
    validateResult(readClient, expectedData, expectedBlockIds4);
  }

  protected void validateResult(ShuffleReadClientImpl readClient, Map<Long, byte[]> expectedData,
      Set<Long> expectedBlockIds) {
    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Set<Long> matched = Sets.newHashSet();
    while (csb.getByteBuffer() != null) {
      for (Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.add(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertEquals(expectedBlockIds.size(), matched.size());
    assertTrue(expectedBlockIds.containsAll(matched));
  }
}
