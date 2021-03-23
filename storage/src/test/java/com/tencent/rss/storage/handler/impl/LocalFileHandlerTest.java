package com.tencent.rss.storage.handler.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.handler.api.ServerReadHandler;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class LocalFileHandlerTest {

  private static AtomicLong ATOMIC_LONG = new AtomicLong(0L);

  @Test
  public void writeTest() throws Exception {
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String[] basePaths = new String[]{dataDir1.getAbsolutePath(),
        dataDir2.getAbsolutePath()};
    LocalFileWriteHandler writeHandler1 = new LocalFileWriteHandler("appId", 0, 1, 1,
        basePaths, "pre");
    LocalFileWriteHandler writeHandler2 = new LocalFileWriteHandler("appId", 0, 2, 2,
        basePaths, "pre");

    String possiblePath1 = ShuffleStorageUtils.getFullShuffleDataFolder(dataDir1.getAbsolutePath(),
        ShuffleStorageUtils.getShuffleDataPath("appId", 0, 1, 1));
    String possiblePath2 = ShuffleStorageUtils.getFullShuffleDataFolder(dataDir2.getAbsolutePath(),
        ShuffleStorageUtils.getShuffleDataPath("appId", 0, 1, 1));
    assertTrue(writeHandler1.getBasePath().endsWith(possiblePath1) ||
        writeHandler1.getBasePath().endsWith(possiblePath2));

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds1 = Sets.newHashSet();
    Set<Long> expectedBlockIds2 = Sets.newHashSet();

    writeTestData(writeHandler1, 1, 32, expectedData, expectedBlockIds1);
    writeTestData(writeHandler1, 2, 32, expectedData, expectedBlockIds1);
    writeTestData(writeHandler1, 3, 32, expectedData, expectedBlockIds1);
    writeTestData(writeHandler1, 4, 32, expectedData, expectedBlockIds1);

    writeTestData(writeHandler2, 3, 32, expectedData, expectedBlockIds2);
    writeTestData(writeHandler2, 3, 32, expectedData, expectedBlockIds2);
    writeTestData(writeHandler2, 2, 32, expectedData, expectedBlockIds2);
    writeTestData(writeHandler2, 1, 32, expectedData, expectedBlockIds2);

    RssBaseConf conf = new RssBaseConf();
    conf.setString("rss.storage.basePath", dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath());
    LocalFileServerReadHandler readHandler1 = new LocalFileServerReadHandler(
        "appId", 0, 1, 1, 10, 1000, expectedBlockIds1, conf);
    LocalFileServerReadHandler readHandler2 = new LocalFileServerReadHandler(
        "appId", 0, 2, 1, 10, 1000, expectedBlockIds2, conf);

    validateResult(readHandler1, expectedBlockIds1, expectedData);
    validateResult(readHandler2, expectedBlockIds2, expectedData);
  }


  private void writeTestData(
      ShuffleWriteHandler writeHandler,
      int num, int length,
      Map<Long, byte[]> expectedData,
      Set<Long> expectedBlockIds) throws Exception {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = ATOMIC_LONG.incrementAndGet();
      blocks.add(new ShufflePartitionedBlock(length, length, ChecksumUtils.getCrc32(buf), blockId, buf));
      expectedData.put(blockId, buf);
      expectedBlockIds.add(blockId);
    }
    writeHandler.write(blocks);
  }

  protected void validateResult(ServerReadHandler readHandler, Set<Long> expectedBlockIds,
      Map<Long, byte[]> expectedData) {
    ShuffleDataResult sdr = readHandler.getShuffleData(expectedBlockIds);
    byte[] buffer = sdr.getData();
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    for (BufferSegment bs : bufferSegments) {
      byte[] data = new byte[bs.getLength()];
      System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
      assertEquals(bs.getCrc(), ChecksumUtils.getCrc32(data));
      assertTrue(Arrays.equals(data, expectedData.get(bs.getBlockId())));
    }
  }
}
