package com.tencent.rss.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import com.tencent.rss.storage.HdfsTestBase;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class FileBasedShuffleReadClientTest extends HdfsTestBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static AtomicLong ATOMIC_LONG = new AtomicLong(0);

  @Test
  public void readTest1() throws Exception {
    String basePath = HDFS_URI + "clientReadTest1";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test1", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 30, expectedData,
        expectedBlockIds);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, expectedBlockIds);
    readClient.checkExpectedBlockIds();

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    expectedBlockIds.add(-1L);
    // can't find all expected block id, data loss
    readClient = new FileBasedShuffleReadClient(basePath, conf, 100, expectedBlockIds);
    try {
      readClient.checkExpectedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Can't find blockIds"));
    } finally {
      readClient.close();
    }
  }

  @Test
  public void readTest2() throws Exception {
    String basePath = HDFS_URI + "clientReadTest2";
    FileBasedShuffleWriteHandler writeHandler1 =
        new FileBasedShuffleWriteHandler(basePath, "test2_1", conf);
    FileBasedShuffleWriteHandler writeHandler2 =
        new FileBasedShuffleWriteHandler(basePath, "test2_2", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler1, 2, 30, expectedData,
        expectedBlockIds);
    writeTestData(writeHandler2, 2, 30, expectedData,
        expectedBlockIds);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, expectedBlockIds);
    readClient.checkExpectedBlockIds();

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest3() throws Exception {
    String basePath = HDFS_URI + "clientReadTest3";
    FileBasedShuffleWriteHandler writeHandler1 =
        new FileBasedShuffleWriteHandler(basePath, "test3_1", conf);
    FileBasedShuffleWriteHandler writeHandler2 =
        new FileBasedShuffleWriteHandler(basePath, "test3_2", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler1, 2, 30, expectedData,
        expectedBlockIds);
    writeTestData(writeHandler2, 2, 30, expectedData,
        expectedBlockIds);

    // duplicate file created, it should be used in product environment
    FileUtil.copy(fs, new Path(basePath + "/test3_1.data"), fs,
        new Path(basePath + "/test3_1.cp.data"), false, conf);
    FileUtil.copy(fs, new Path(basePath + "/test3_1.index"), fs,
        new Path(basePath + "/test3_1.cp.index"), false, conf);
    FileUtil.copy(fs, new Path(basePath + "/test3_2.data"), fs,
        new Path(basePath + "/test3_2.cp.data"), false, conf);
    FileUtil.copy(fs, new Path(basePath + "/test3_2.index"), fs,
        new Path(basePath + "/test3_2.cp.index"), false, conf);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, expectedBlockIds);
    readClient.checkExpectedBlockIds();

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest4() throws Exception {
    String basePath = HDFS_URI + "clientReadTest4";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test1", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 30, expectedData,
        expectedBlockIds);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, expectedBlockIds);
    readClient.checkExpectedBlockIds();
    // data file is deleted after readClient checkExpectedBlockIds
    fs.delete(new Path(basePath + "/test1.data"), true);
    // sleep to wait delete operation
    Thread.sleep(10000);

    try {
      readClient.readShuffleData();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Can't read data with blockId"));
    }
    readClient.close();
  }

  @Test
  public void readTest5() throws Exception {
    String basePath = HDFS_URI + "clientReadTest5";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 30, expectedData,
        expectedBlockIds);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, expectedBlockIds);
    readClient.checkExpectedBlockIds();
    // index file is deleted after iterator initialization, it should be ok, all index infos are read already
    fs.delete(new Path(basePath + "/test.index"), true);
    // sleep to wait delete operation
    Thread.sleep(10000);

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();
  }

  @Test
  public void readTest6() throws Exception {
    String basePath = HDFS_URI + "clientReadTest6";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 30, expectedData,
        expectedBlockIds);
    // index file is deleted before iterator initialization
    fs.delete(new Path(basePath + "/test.index"), true);
    // sleep to wait delete operation
    Thread.sleep(10000);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, expectedBlockIds);
    try {
      readClient.checkExpectedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("No index file found"));
    }
    readClient.close();
  }

  @Test
  public void readTest7() throws Exception {
    String basePath = HDFS_URI + "clientReadTest7";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 5, 30, expectedData,
        expectedBlockIds);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, expectedBlockIds);
    readClient.checkExpectedBlockIds();

    // discard one block
    readClient.getBlockIdQueue().poll();

    byte[] data = readClient.readShuffleData();
    while (data != null) {
      data = readClient.readShuffleData();
    }
    try {
      readClient.checkProcessedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Blocks read inconsistent:"));
    }
    readClient.close();
  }

  @Test
  public void readTest8() throws Exception {
    String basePath = HDFS_URI + "clientReadTest8";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 30, expectedData,
        expectedBlockIds);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, expectedBlockIds);
    readClient.checkExpectedBlockIds();
    // crc32 is incorrect
    try (MockedStatic<ChecksumUtils> checksumUtilsMock = Mockito.mockStatic(ChecksumUtils.class)) {
      checksumUtilsMock.when(() -> ChecksumUtils.getCrc32(any())).thenReturn(-1L);
      try {
        byte[] data = readClient.readShuffleData();
        while (data != null) {
          data = readClient.readShuffleData();
        }
        fail(EXPECTED_EXCEPTION_MESSAGE);
      } catch (Exception e) {
        assertTrue(e.getMessage().startsWith("Unexpected crc value"));
      }
    }
    readClient.close();
  }

  private void writeTestData(
      FileBasedShuffleWriteHandler writeHandler,
      int num, int length,
      Map<Long, byte[]> expectedData,
      Set<Long> expectedBlockIds) throws Exception {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = ATOMIC_LONG.incrementAndGet();
      blocks.add(new ShufflePartitionedBlock(length, ChecksumUtils.getCrc32(buf), blockId, buf));
      expectedData.put(blockId, buf);
      expectedBlockIds.add(blockId);
    }
    writeHandler.write(blocks);
  }

  protected void validateResult(ShuffleReadClient readClient,
      Map<Long, byte[]> expectedData) {
    byte[] data = readClient.readShuffleData();
    int blockNum = 0;
    while (data != null) {
      blockNum++;
      boolean match = false;
      for (byte[] expected : expectedData.values()) {
        if (Arrays.equals(data, expected)) {
          match = true;
        }
      }
      assertTrue(match);
      data = readClient.readShuffleData();
    }
    assertEquals(expectedData.size(), blockNum);
  }
}
