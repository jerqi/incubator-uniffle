package com.tencent.rss.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.impl.HdfsShuffleWriteHandler;
import com.tencent.rss.storage.util.StorageType;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class ShuffleReadClientImplTest extends HdfsTestBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static AtomicLong ATOMIC_LONG = new AtomicLong(0);
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void readTest1() throws Exception {
    String basePath = HDFS_URI + "clientReadTest1";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 1, 1, basePath, "test1", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData,
        blockIdBitmap);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 100, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    blockIdBitmap.addLong(-1L);
    try {
      // can't find all expected block id, data loss
      readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 100, 1,
          10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("BlockId is incorrect"));
    } finally {
      readClient.close();
    }
  }

  @Test
  public void readTest2() throws Exception {
    String basePath = HDFS_URI + "clientReadTest2";
    HdfsShuffleWriteHandler writeHandler1 =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test2_1", conf);
    HdfsShuffleWriteHandler writeHandler2 =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test2_2", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler1, 2, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler2, 2, 30, 0, expectedData, blockIdBitmap);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        "appId", 0, 1, 100, 2, 10, 1000,
        basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest3() throws Exception {
    String basePath = HDFS_URI + "clientReadTest3";
    HdfsShuffleWriteHandler writeHandler1 =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test3_1", conf);
    HdfsShuffleWriteHandler writeHandler2 =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test3_2", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler1, 2, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler2, 2, 30, 0, expectedData, blockIdBitmap);

    // duplicate file created, it should be used in product environment
    String shuffleFolder = basePath + "/appId/0/0-1";
    FileUtil.copy(fs, new Path(shuffleFolder + "/test3_1_0.data"), fs,
        new Path(basePath + "/test3_1.cp.data"), false, conf);
    FileUtil.copy(fs, new Path(shuffleFolder + "/test3_1_0.index"), fs,
        new Path(basePath + "/test3_1.cp.index"), false, conf);
    FileUtil.copy(fs, new Path(shuffleFolder + "/test3_2_0.data"), fs,
        new Path(basePath + "/test3_2.cp.data"), false, conf);
    FileUtil.copy(fs, new Path(shuffleFolder + "/test3_2_0.index"), fs,
        new Path(basePath + "/test3_2.cp.index"), false, conf);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        "appId", 0, 1, 100, 2, 10, 1000,
        basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest4() throws Exception {
    String basePath = HDFS_URI + "clientReadTest4";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test1", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData, blockIdBitmap);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        "appId", 0, 1, 100, 2, 10, 1000,
        basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());
    Path dataFile = new Path(basePath + "/appId/0/0-1/test1_0.data");
    // data file is deleted after readClient checkExpectedBlockIds
    fs.delete(new Path(basePath + "/appId/0/0-1/test1_0.data"), true);
    // sleep to wait delete operation
    Thread.sleep(10000);

    assertNull(readClient.readShuffleBlockData());
    try {
      fs.listStatus(dataFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
    }

    try {
      readClient.checkProcessedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Blocks read inconsistent: expected"));
    }
    readClient.close();
  }

  @Test
  public void readTest5() throws Exception {
    String basePath = HDFS_URI + "clientReadTest5";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData, blockIdBitmap);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        "appId", 0, 1, 100, 2, 10, 1000,
        basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());
    // index file is deleted after iterator initialization, it should be ok, all index infos are read already
    Path indexFile = new Path(basePath + "/appId/0/0-1/test.index");
    fs.delete(indexFile, true);
    // sleep to wait delete operation
    Thread.sleep(10000);
    try {
      fs.listStatus(indexFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
    }

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest6() throws Exception {
    String basePath = HDFS_URI + "clientReadTest6";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData, blockIdBitmap);
    Path indexFile = new Path(basePath + "/appId/0/0-1/test_0.index");
    // index file is deleted before iterator initialization
    fs.delete(indexFile, true);
    // sleep to wait delete operation
    Thread.sleep(10000);
    try {
      fs.listStatus(indexFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
    }

    try {
      new ShuffleReadClientImpl(StorageType.HDFS.name(),
          "appId", 0, 1, 100, 2, 10, 1000,
          basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Can't list index"));
    }
  }

  @Test
  public void readTest7() throws Exception {
    String basePath = HDFS_URI + "clientReadTest7";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);

    Map<Long, byte[]> expectedData1 = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 10, 30, 0, expectedData1, blockIdBitmap1);

    Map<Long, byte[]> expectedData2 = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    writeTestData(writeHandler, 10, 30, 0, expectedData2, blockIdBitmap2);

    writeTestData(writeHandler, 10, 30, 0, expectedData1, blockIdBitmap1);

    ShuffleReadClientImpl readClient1 = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        "appId", 0, 0, 100, 2, 10, 100,
        basePath, blockIdBitmap1, taskIdBitmap, Lists.newArrayList(), new Configuration());
    ShuffleReadClientImpl readClient2 = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        "appId", 0, 1, 100, 2, 10, 100,
        basePath, blockIdBitmap2, taskIdBitmap, Lists.newArrayList(), new Configuration());
    validateResult(readClient1, expectedData1);
    readClient1.checkProcessedBlockIds();
    readClient1.close();

    validateResult(readClient2, expectedData2);
    readClient2.checkProcessedBlockIds();
    readClient2.close();
  }

  @Test
  public void readTest8() throws Exception {
    String basePath = HDFS_URI + "clientReadTest8";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 2, 30, 0, expectedData, blockIdBitmap);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        "appId", 0, 1, 100, 2, 10, 1000,
        basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());
    // crc32 is incorrect
    try (MockedStatic<ChecksumUtils> checksumUtilsMock = Mockito.mockStatic(ChecksumUtils.class)) {
      checksumUtilsMock.when(() -> ChecksumUtils.getCrc32((ByteBuffer) any())).thenReturn(-1L);
      try {
        ByteBuffer bb = readClient.readShuffleBlockData().getByteBuffer();
        while (bb != null) {
          bb = readClient.readShuffleBlockData().getByteBuffer();
        }
        fail(EXPECTED_EXCEPTION_MESSAGE);
      } catch (Exception e) {
        assertTrue(e.getMessage().startsWith("Unexpected crc value"));
      }
    }
    readClient.close();
  }

  @Test
  public void readTest9() {
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
        "appId", 0, 1, 100, 2, 10, 1000,
        "basePath", Roaring64NavigableMap.bitmapOf(), Roaring64NavigableMap.bitmapOf(),
        Lists.newArrayList(), new Configuration());
    assertNull(readClient.readShuffleBlockData().getByteBuffer());
  }

  @Test
  public void readTest10() throws Exception {
    String basePath = HDFS_URI + "clientReadTest10";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    Roaring64NavigableMap wrongBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    LongIterator iter = blockIdBitmap.getLongIterator();
    while (iter.hasNext()) {
      wrongBlockIdBitmap.addLong(iter.next() + 1000);
    }

    try {
      new ShuffleReadClientImpl(StorageType.HDFS.name(),
          "appId", 0, 0, 100, 2, 10, 100,
          basePath, wrongBlockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("BlockId is incorrect"));
    }
  }

  @Test
  public void readTest11() throws Exception {
    String basePath = HDFS_URI + "clientReadTest11";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 1, 1, basePath, "test1", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    writeTestData(writeHandler, 10, 30, 0, expectedData, blockIdBitmap);

    // test with different indexReadLimit to validate result
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 1, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 2, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 3, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 10, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 11, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest12() throws Exception {
    String basePath = HDFS_URI + "clientReadTest12";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 1, 1, basePath, "test1", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 1);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 1, expectedData, blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 2, Maps.newHashMap(), blockIdBitmap);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 100, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    assertEquals(15, readClient.getProcessedBlockIds().getLongCardinality());
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest13() throws Exception {
    String basePath = HDFS_URI + "clientReadTest13";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 1, 1, basePath, "test1", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 3);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    // test case: data generated by speculation task without report result
    writeTestData(writeHandler, 5, 30, 1, Maps.newHashMap(), Roaring64NavigableMap.bitmapOf());
    // test case: data generated by speculation task with report result
    writeTestData(writeHandler, 5, 30, 2, Maps.newHashMap(), blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 3, expectedData, blockIdBitmap);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 100, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    assertEquals(20, readClient.getProcessedBlockIds().getLongCardinality());
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest14() throws Exception {
    String basePath = HDFS_URI + "clientReadTest14";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 1, 1, basePath, "test1", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0, 2);
    writeTestData(writeHandler, 5, 30, 0, expectedData, blockIdBitmap);
    writeTestData(writeHandler, 5, 30, 1, Maps.newHashMap(), Roaring64NavigableMap.bitmapOf());
    writeTestData(writeHandler, 5, 30, 2, expectedData, blockIdBitmap);

    // unexpected taskAttemptId should be filtered
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(), "appId", 0, 1, 100, 1,
        10, 1000, basePath, blockIdBitmap, taskIdBitmap, Lists.newArrayList(), new Configuration());

    validateResult(readClient, expectedData);
    assertEquals(15, readClient.getProcessedBlockIds().getLongCardinality());
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  private void writeTestData(
      HdfsShuffleWriteHandler writeHandler,
      int num, int length, long taskAttemptId,
      Map<Long, byte[]> expectedData,
      Roaring64NavigableMap blockIdBitmap) throws Exception {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = ATOMIC_LONG.incrementAndGet();
      blocks.add(new ShufflePartitionedBlock(
          length, length, ChecksumUtils.getCrc32(buf), blockId, taskAttemptId, ByteBuffer.wrap(buf)));
      expectedData.put(blockId, buf);
      blockIdBitmap.addLong(blockId);
    }
    writeHandler.write(blocks);
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
      data = readClient.readShuffleBlockData().getByteBuffer();
    }
    assertEquals(expectedData.size(), blockNum);
  }

  private boolean compareByte(byte[] expected, ByteBuffer buffer) {
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != buffer.get(i)) {
        return false;
      }
    }
    return true;
  }
}
