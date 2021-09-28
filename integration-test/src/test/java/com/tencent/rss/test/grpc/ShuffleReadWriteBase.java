package com.tencent.rss.test.grpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.tencent.rss.client.TestUtils;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.test.IntegrationTestBase;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public abstract class ShuffleReadWriteBase extends IntegrationTestBase {

  private static AtomicLong ATOMIC_LONG = new AtomicLong(0L);
  protected List<ShuffleServerInfo> mockSSI =
      Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));

  protected List<ShuffleBlockInfo> createShuffleBlockList(int shuffleId, int partitionId, long taskAttemptId,
      int blockNum, int length, Roaring64NavigableMap blockIdBitmap, Map<Long, byte[]> dataMap,
      List<ShuffleServerInfo> shuffleServerInfoList) {
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = ATOMIC_LONG.getAndIncrement();
      blockIdBitmap.addLong(blockId);
      dataMap.put(blockId, buf);
      shuffleBlockInfoList.add(new ShuffleBlockInfo(
          shuffleId, partitionId, blockId, length, ChecksumUtils.getCrc32(buf),
          buf, shuffleServerInfoList, length, 10, taskAttemptId));
    }
    return shuffleBlockInfoList;
  }

  protected Map<Integer, List<ShuffleBlockInfo>> createTestData(
      Roaring64NavigableMap[] bitmaps,
      Map<Long, byte[]> expectedData) {
    for (int i = 0; i < 4; i++) {
      bitmaps[i] = Roaring64NavigableMap.bitmapOf();
    }
    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        0, 0, 0, 3, 25, bitmaps[0], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        0, 1, 1, 5, 25, bitmaps[1], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        0, 2, 2, 4, 25, bitmaps[2], expectedData, mockSSI);
    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        0, 3, 3, 1, 25, bitmaps[3], expectedData, mockSSI);
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    partitionToBlocks.put(1, blocks2);
    partitionToBlocks.put(2, blocks3);
    partitionToBlocks.put(3, blocks4);
    return partitionToBlocks;
  }

  protected List<ShuffleBlockInfo> createShuffleBlockList(int shuffleId, int partitionId, long taskAttemptId,
      int blockNum, int length, Roaring64NavigableMap blockIdBitmap, Map<Long, byte[]> dataMap) {
    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    return createShuffleBlockList(
        shuffleId, partitionId, taskAttemptId, blockNum, length, blockIdBitmap, dataMap, shuffleServerInfoList);
  }

  protected boolean compareByte(byte[] expected, ByteBuffer buffer) {
    return TestUtils.compareByte(expected, buffer);
  }

  protected void validateResult(ShuffleReadClient readClient, Map<Long, byte[]> expectedData) {
    TestUtils.validateResult(readClient, expectedData);
  }

  protected static String generateBasePath() {
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    tmpDir.deleteOnExit();
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    return basePath;
  }
}
