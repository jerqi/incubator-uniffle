package com.tencent.rss.test.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.ShuffleWriteClientImpl;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.client.util.ClientType;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class ShuffleWithRssClientTest extends ShuffleReadWriteBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static ShuffleServerInfo shuffleServerInfo1;
  private static ShuffleServerInfo shuffleServerInfo2;
  private ShuffleWriteClientImpl shuffleWriteClientImpl;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    createShuffleServer(shuffleServerConf);
    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    basePath = dataDir3.getAbsolutePath() + "," + dataDir4.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 1);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18081);
    createShuffleServer(shuffleServerConf);
    startServers();
    shuffleServerInfo1 =
        new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(0).getIp(), SHUFFLE_SERVER_PORT);
    shuffleServerInfo2 =
        new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(1).getIp(), SHUFFLE_SERVER_PORT + 1);
  }

  @Before
  public void createClient() {
    shuffleWriteClientImpl = new ShuffleWriteClientImpl(ClientType.GRPC.name(), 3, 1000, 1);
  }

  @After
  public void closeClient() {
    shuffleWriteClientImpl.close();
  }

  @Test
  public void rpcFailTest() throws Exception {
    String testAppId = "rpcFailTest";
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo1,
        testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    // simulator a failed server
    ShuffleServerInfo fakeShuffleServerInfo =
        new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(0).getIp(), SHUFFLE_SERVER_PORT + 100);
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 3, 25, blockIdBitmap,
        expectedData, Lists.newArrayList(shuffleServerInfo1, fakeShuffleServerInfo));
    SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    Roaring64NavigableMap failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    assertEquals(blockIdBitmap, failedBlockIdBitmap);
    assertEquals(blockIdBitmap, succBlockIdBitmap);

    boolean commitResult = shuffleWriteClientImpl.sendCommit(Sets.newHashSet(
        shuffleServerInfo1, fakeShuffleServerInfo), testAppId, 0, 2);
    assertFalse(commitResult);

    Map<Integer, List<Long>> ptb = Maps.newHashMap();
    ptb.put(1, Lists.newArrayList(1L));
    try {
      shuffleWriteClientImpl.reportShuffleResult(Sets.newHashSet(
          shuffleServerInfo1, fakeShuffleServerInfo), testAppId, 0, 0, ptb, 2);
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Report shuffle result is failed for"));
    }
  }

  @Test
  public void writeReadTest() throws Exception {
    String testAppId = "writeReadTest";
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo1,
        testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo2,
        testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);

    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
        0, 0, 0, 3, 25, blockIdBitmap,
        expectedData, Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2));
    shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    // send 1st commit, finish commit won't be sent to Shuffle server and data won't be persisted to disk
    boolean commitResult = shuffleWriteClientImpl
        .sendCommit(Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertTrue(commitResult);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", blockIdBitmap, taskIdBitmap,
        Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2), null);

    try {
      readClient.readShuffleBlockData();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to read shuffle data for"));
    }
    readClient.close();

    // send 2nd commit, data will be persisted to disk
    commitResult = shuffleWriteClientImpl
        .sendCommit(Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertTrue(commitResult);
    readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(), testAppId, 0, 0, 100, 1,
        10, 1000, "", blockIdBitmap, taskIdBitmap,
        Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2), null);
    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    // commit will be failed because of fakeIp
    commitResult = shuffleWriteClientImpl.sendCommit(Sets.newHashSet(new ShuffleServerInfo(
        "127.0.0.1-20001", "fakeIp", SHUFFLE_SERVER_PORT)), testAppId, 0, 2);
    assertFalse(commitResult);

    // wait resource to be deleted
    Thread.sleep(6000);

    // commit is ok, but finish shuffle rpc will failed because resource was deleted
    commitResult = shuffleWriteClientImpl
        .sendCommit(Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2), testAppId, 0, 2);
    assertFalse(commitResult);
  }

  protected void validateResult(ShuffleReadClient readClient,
      Map<Long, byte[]> expectedData) {
    ByteBuffer data = readClient.readShuffleBlockData().getByteBuffer();
    int blockNum = 0;
    while (data != null) {
      blockNum++;
      boolean match = false;
      for (byte[] expected : expectedData.values()) {
        if (compareByte(expected, data)) {
          match = true;
        }
      }
      assertTrue(match);
      CompressedShuffleBlock csb = readClient.readShuffleBlockData();
      if (csb == null) {
        data = null;
      } else {
        data = csb.getByteBuffer();
      }
    }
    assertEquals(expectedData.size(), blockNum);
  }

}
