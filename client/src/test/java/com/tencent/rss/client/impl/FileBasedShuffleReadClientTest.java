package com.tencent.rss.client.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.impl.FileBasedShuffleReadClient.FileReadSegment;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.FileBasedShuffleSegment;
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
        basePath, conf, 100, 31, expectedBlockIds);
    readClient.checkExpectedBlockIds();

    validateResult(readClient, expectedData);
    readClient.checkProcessedBlockIds();
    readClient.close();

    expectedBlockIds.add(-1L);
    // can't find all expected block id, data loss
    readClient = new FileBasedShuffleReadClient(basePath, conf, 100, 1000, expectedBlockIds);
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
        basePath, conf, 100, 1000, expectedBlockIds);
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
        basePath, conf, 100, 10000, expectedBlockIds);
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
        basePath, conf, 100, 10000, expectedBlockIds);
    readClient.checkExpectedBlockIds();
    // data file is deleted after readClient checkExpectedBlockIds
    fs.delete(new Path(basePath + "/test1.data"), true);
    // sleep to wait delete operation
    Thread.sleep(10000);
    assertNull(readClient.readShuffleData());

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
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 30, expectedData,
        expectedBlockIds);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
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
        basePath, conf, 100, 10000, expectedBlockIds);
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
        basePath, conf, 100, 50, expectedBlockIds);
    readClient.checkExpectedBlockIds();

    byte[] data = readClient.readShuffleData();
    while (data != null) {
      data = readClient.readShuffleData();
      // discard block
      readClient.getBlockIdQueue().poll();
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
        basePath, conf, 100, 10000, expectedBlockIds);
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

  @Test
  public void readTest9() {
    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        "basePath", conf, 100, 10000, Sets.newHashSet());
    readClient.checkExpectedBlockIds();
    assertNull(readClient.readShuffleData());
  }

  @Test
  public void mergeSegmentsTest() throws Exception {
    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        "basePath", conf, 100, 100, Sets.newHashSet());

    List<FileBasedShuffleSegment> segments = Lists.newArrayList(
        new FileBasedShuffleSegment(0, 40, 0, 1));
    List<FileReadSegment> fileSegments = readClient.mergeSegments("path", segments);
    assertEquals(1, fileSegments.size());
    for (FileReadSegment seg : fileSegments) {
      assertEquals(0, seg.offset);
      assertEquals(40, seg.length);
      assertEquals("path", seg.path);
      Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
      assertEquals(1, blockIdToBufferSegment.size());
      assertEquals(new BufferSegment(0, 40, 0), blockIdToBufferSegment.get(1L));
    }

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(0, 40, 0, 1),
        new FileBasedShuffleSegment(40, 40, 0, 2),
        new FileBasedShuffleSegment(80, 20, 0, 3));
    fileSegments = readClient.mergeSegments("path", segments);
    assertEquals(1, fileSegments.size());
    for (FileReadSegment seg : fileSegments) {
      assertEquals(0, seg.offset);
      assertEquals(100, seg.length);
      assertEquals("path", seg.path);
      Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
      assertEquals(3, blockIdToBufferSegment.size());
      assertEquals(new BufferSegment(0, 40, 0), blockIdToBufferSegment.get(1L));
      assertEquals(new BufferSegment(40, 40, 0), blockIdToBufferSegment.get(2L));
      assertEquals(new BufferSegment(80, 20, 0), blockIdToBufferSegment.get(3L));
    }

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(0, 40, 0, 1),
        new FileBasedShuffleSegment(40, 40, 0, 2),
        new FileBasedShuffleSegment(80, 20, 0, 3),
        new FileBasedShuffleSegment(100, 20, 0, 4));
    fileSegments = readClient.mergeSegments("path", segments);
    assertEquals(2, fileSegments.size());
    boolean tested = false;
    for (FileReadSegment seg : fileSegments) {
      if (seg.offset == 100) {
        tested = true;
        assertEquals(20, seg.length);
        assertEquals("path", seg.path);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 20, 0), blockIdToBufferSegment.get(4L));
      }
    }
    assertTrue(tested);

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(0, 40, 0, 1),
        new FileBasedShuffleSegment(40, 40, 0, 2),
        new FileBasedShuffleSegment(80, 20, 0, 3),
        new FileBasedShuffleSegment(100, 20, 0, 4),
        new FileBasedShuffleSegment(120, 100, 0, 5));
    fileSegments = readClient.mergeSegments("path", segments);
    assertEquals(2, fileSegments.size());
    tested = false;
    for (FileReadSegment seg : fileSegments) {
      if (seg.offset == 100) {
        tested = true;
        assertEquals(120, seg.length);
        assertEquals("path", seg.path);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(2, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 20, 0), blockIdToBufferSegment.get(4L));
        assertEquals(new BufferSegment(20, 100, 0), blockIdToBufferSegment.get(5L));
      }
    }
    assertTrue(tested);

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(10, 40, 0, 1),
        new FileBasedShuffleSegment(80, 20, 0, 2),
        new FileBasedShuffleSegment(500, 120, 0, 3),
        new FileBasedShuffleSegment(700, 20, 0, 4));
    fileSegments = readClient.mergeSegments("path", segments);
    assertEquals(3, fileSegments.size());
    Set<Long> expectedOffset = Sets.newHashSet(10L, 500L, 700L);
    for (FileReadSegment seg : fileSegments) {
      if (seg.offset == 10) {
        assertEquals(90, seg.length);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(2, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 40, 0), blockIdToBufferSegment.get(1L));
        assertEquals(new BufferSegment(70, 20, 0), blockIdToBufferSegment.get(2L));
        expectedOffset.remove(10L);
      }
      if (seg.offset == 500) {
        assertEquals(120, seg.length);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 120, 0), blockIdToBufferSegment.get(3L));
        expectedOffset.remove(500L);
      }
      if (seg.offset == 700) {
        assertEquals(20, seg.length);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 20, 0), blockIdToBufferSegment.get(4L));
        expectedOffset.remove(700L);
      }
    }
    assertTrue(expectedOffset.isEmpty());

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(500, 120, 0, 5),
        new FileBasedShuffleSegment(630, 10, 0, 3),
        new FileBasedShuffleSegment(80, 20, 0, 2),
        new FileBasedShuffleSegment(10, 40, 0, 1),
        new FileBasedShuffleSegment(769, 20, 0, 6),
        new FileBasedShuffleSegment(700, 20, 0, 4));
    fileSegments = readClient.mergeSegments("path", segments);
    assertEquals(4, fileSegments.size());
    expectedOffset = Sets.newHashSet(10L, 500L, 630L, 700L);
    for (FileReadSegment seg : fileSegments) {
      if (seg.offset == 10) {
        assertEquals(90, seg.length);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(2, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 40, 0), blockIdToBufferSegment.get(1L));
        assertEquals(new BufferSegment(70, 20, 0), blockIdToBufferSegment.get(2L));
        expectedOffset.remove(10L);
      }
      if (seg.offset == 500) {
        assertEquals(120, seg.length);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 120, 0), blockIdToBufferSegment.get(5L));
        expectedOffset.remove(500L);
      }
      if (seg.offset == 630) {
        assertEquals(10, seg.length);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 10, 0), blockIdToBufferSegment.get(3L));
        expectedOffset.remove(630L);
      }
      if (seg.offset == 700) {
        assertEquals(89, seg.length);
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.blockIdToBufferSegment;
        assertEquals(2, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 20, 0), blockIdToBufferSegment.get(4L));
        assertEquals(new BufferSegment(69, 20, 0), blockIdToBufferSegment.get(6L));
        expectedOffset.remove(700L);
      }
    }
    assertTrue(expectedOffset.isEmpty());
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
