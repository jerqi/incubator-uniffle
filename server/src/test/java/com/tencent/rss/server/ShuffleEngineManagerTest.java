package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.impl.HdfsClientReadHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShuffleEngineManagerTest extends HdfsTestBase {

  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();
  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  private ShuffleEngineManager shuffleEngineManager = new ShuffleEngineManager("test", 1);
  private ShuffleEngine mockShuffleEngine = mock(ShuffleEngine.class);

  @BeforeClass
  public static void beforeAll() {
    ShuffleServerMetrics.register();
  }

  @Test
  public void registerShuffleEngineTest() {
    when(mockShuffleEngine.init()).thenReturn(StatusCode.SUCCESS);
    StatusCode actual = shuffleEngineManager.registerShuffleEngine(1, 10, mockShuffleEngine);
    StatusCode expected = StatusCode.SUCCESS;
    assertEquals(expected, actual);
  }

  @Test
  public void getShuffleEngineTest() {
    when(mockShuffleEngine.init()).thenReturn(StatusCode.SUCCESS);
    shuffleEngineManager.registerShuffleEngine(1, 10, mockShuffleEngine);
    ShuffleEngine actual1 = shuffleEngineManager.getShuffleEngine(1);
    assertEquals(mockShuffleEngine, actual1);
    actual1 = shuffleEngineManager.getShuffleEngine(10);
    assertEquals(mockShuffleEngine, actual1);
    ShuffleEngine actual2 = shuffleEngineManager.getShuffleEngine(11);
    assertEquals(null, actual2);
  }

  @Test
  public void writeProcessTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    String storageBasePath = HDFS_URI + "rss/test";
    String appId = "testAppId";
    int shuffleId = 1;
    conf.setString("rss.server.buffer.capacity", "64");
    conf.setString("rss.server.buffer.size", "64");
    conf.setString("rss.storage.basePath", storageBasePath);
    conf.setString("rss.storage.type", "HDFS");
    conf.setString("rss.server.commit.timeout", "10000");
    ShuffleServer shuffleServer = new ShuffleServer(conf);
    BufferManager bufferManager = shuffleServer.getBufferManager();
    String serverId = shuffleServer.getId();
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    ShuffleEngineManager shuffleEngineManager = new ShuffleEngineManager(
        appId, shuffleId, conf, bufferManager, shuffleFlushManager);
    shuffleEngineManager.registerShuffleEngine(0, 1);
    shuffleEngineManager.registerShuffleEngine(2, 3);
    List<ShufflePartitionedBlock> expectedBlocks0 = Lists.newArrayList();
    List<ShufflePartitionedBlock> expectedBlocks1 = Lists.newArrayList();
    List<ShufflePartitionedBlock> expectedBlocks2 = Lists.newArrayList();
    List<ShufflePartitionedBlock> expectedBlocks3 = Lists.newArrayList();
    String shuffleFilePath1 = ShuffleStorageUtils.getShuffleDataPath(appId, shuffleId, 0, 1);
    String shuffleFilePath2 = ShuffleStorageUtils.getShuffleDataPath(appId, shuffleId, 2, 3);

    shuffleEngineManager.commit();

    // won't flush for partition 0-1
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
    expectedBlocks1.addAll(partitionedData0.getBlockList());
    StatusCode sc = shuffleEngineManager.getShuffleEngine(1).write(partitionedData0);
    assertEquals(StatusCode.SUCCESS, sc);

    shuffleEngineManager.commit();
    assertEquals(1, shuffleFlushManager.getEventIds(shuffleFilePath1).size());
    assertNull(shuffleFlushManager.getEventIds(shuffleFilePath2));

    // flush for partition 0-1
    ShufflePartitionedData partitionedData1 = createPartitionedData(0, 2, 35);
    expectedBlocks0.addAll(partitionedData1.getBlockList());
    sc = shuffleEngineManager.getShuffleEngine(0).write(partitionedData1);
    assertEquals(StatusCode.SUCCESS, sc);
    waitForFlush(shuffleFlushManager, shuffleFilePath1, 2, false);

    // won't flush for partition 0-1
    ShufflePartitionedData partitionedData2 = createPartitionedData(1, 1, 35);
    expectedBlocks1.addAll(partitionedData2.getBlockList());
    sc = shuffleEngineManager.getShuffleEngine(1).write(partitionedData2);
    assertEquals(StatusCode.SUCCESS, sc);

    // won't flush for partition 2-3
    ShufflePartitionedData partitionedData3 = createPartitionedData(2, 1, 35);
    expectedBlocks2.addAll(partitionedData3.getBlockList());
    sc = shuffleEngineManager.getShuffleEngine(2).write(partitionedData3);
    assertEquals(StatusCode.SUCCESS, sc);

    // flush for partition 2-3
    ShufflePartitionedData partitionedData4 = createPartitionedData(3, 1, 35);
    expectedBlocks3.addAll(partitionedData4.getBlockList());
    sc = shuffleEngineManager.getShuffleEngine(3).write(partitionedData4);
    assertEquals(StatusCode.SUCCESS, sc);

    shuffleEngineManager.commit();
    // 1 event created by flush, 1 event created by commit
    waitForFlush(shuffleFlushManager, shuffleFilePath1, 3, false);
    waitForFlush(shuffleFlushManager, shuffleFilePath2, 1, false);
    assertEquals(3, shuffleFlushManager.getEventIds(shuffleFilePath1).size());
    assertEquals(1, shuffleFlushManager.getEventIds(shuffleFilePath2).size());

    // flush for partition 0-1
    ShufflePartitionedData partitionedData5 = createPartitionedData(0, 2, 35);
    expectedBlocks1.addAll(partitionedData5.getBlockList());
    sc = shuffleEngineManager.getShuffleEngine(0).write(partitionedData5);
    assertEquals(StatusCode.SUCCESS, sc);

    waitForFlush(shuffleFlushManager, shuffleFilePath1, 4, false);
    shuffleEngineManager.commit();
    assertEquals(4, shuffleFlushManager.getEventIds(shuffleFilePath1).size());
    assertEquals(1, shuffleFlushManager.getEventIds(shuffleFilePath2).size());

    shuffleEngineManager.commit();
    assertEquals(4, shuffleFlushManager.getEventIds(shuffleFilePath1).size());
    assertEquals(1, shuffleFlushManager.getEventIds(shuffleFilePath2).size());

    validate(appId, shuffleId, 0, expectedBlocks0, storageBasePath);
    validate(appId, shuffleId, 1, expectedBlocks1, storageBasePath);
    validate(appId, shuffleId, 2, expectedBlocks2, storageBasePath);
    validate(appId, shuffleId, 3, expectedBlocks3, storageBasePath);

    // flush for partition 0-1
    ShufflePartitionedData partitionedData7 = createPartitionedData(0, 2, 35);
    sc = shuffleEngineManager.getShuffleEngine(0).write(partitionedData7);
    assertEquals(StatusCode.SUCCESS, sc);

    waitForFlush(shuffleFlushManager, shuffleFilePath1, 5, true);
    try {
      shuffleEngineManager.commit();
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Shuffle data commit timeout for"));
    }
  }

  private void waitForFlush(ShuffleFlushManager shuffleFlushManager,
      String shuffleFilePath, int eventNum, boolean isClear) throws Exception {
    int retry = 0;
    while (true) {
      // remove flushed eventId to test timeout in commit
      if (shuffleFlushManager.getEventIds(shuffleFilePath).size() == eventNum) {
        if (isClear) {
          shuffleFlushManager.getEventIds(shuffleFilePath).clear();
        }
        break;
      }
      Thread.sleep(1000);
      retry++;
      if (retry > 5) {
        fail("Timeout to flush data");
      }
    }
  }

  private ShufflePartitionedData createPartitionedData(int partitionId, int blockNum, int dataLength) {
    List<ShufflePartitionedBlock> blocks = createBlock(blockNum, dataLength);
    return new ShufflePartitionedData(partitionId, blocks);
  }

  private List<ShufflePartitionedBlock> createBlock(int num, int length) {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      blocks.add(new ShufflePartitionedBlock(
          length, ChecksumUtils.getCrc32(buf), ATOMIC_INT.incrementAndGet(), buf));
    }
    return blocks;
  }

  private void validate(String appId, int shuffleId, int partitionId, List<ShufflePartitionedBlock> blocks,
      String basePath) {
    Set<Long> blockIds = Sets.newHashSet(blocks.stream().map(spb -> spb.getBlockId()).collect(Collectors.toList()));
    HdfsClientReadHandler handler = new HdfsClientReadHandler(appId, shuffleId, partitionId,
        100, 2, 10, 1000, basePath, blockIds);

    ShuffleDataResult sdr = handler.readShuffleData(blockIds);

    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    int matchNum = 0;
    for (ShufflePartitionedBlock block : blocks) {
      for (BufferSegment bs : bufferSegments) {
        if (bs.getBlockId() == block.getBlockId()) {
          assertEquals(block.getLength(), bs.getLength());
          assertEquals(block.getCrc(), bs.getCrc());
          matchNum++;
          break;
        }
      }
    }
    assertEquals(blocks.size(), matchNum);
  }
}
