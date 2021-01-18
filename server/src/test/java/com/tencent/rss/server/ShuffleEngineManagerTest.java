package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.FileBasedShuffleReadHandler;
import com.tencent.rss.storage.FileBasedShuffleSegment;
import com.tencent.rss.storage.HdfsTestBase;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShuffleEngineManagerTest extends HdfsTestBase {

  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();
  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  private ShuffleEngineManager shuffleEngineManager = new ShuffleEngineManager("test", "1");
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
    String shuffleId = "1";
    conf.setString("rss.buffer.capacity", "64");
    conf.setString("rss.buffer.size", "64");
    conf.setString("rss.data.storage.basePath", storageBasePath);
    ShuffleServer shuffleServer = new ShuffleServer(conf);
    BufferManager bufferManager = shuffleServer.getBufferManager();
    String serverId = shuffleServer.getId();
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    ShuffleEngineManager shuffleEngineManager = new ShuffleEngineManager(
        appId, shuffleId, conf, bufferManager, shuffleFlushManager);
    shuffleEngineManager.registerShuffleEngine(0, 1);
    shuffleEngineManager.registerShuffleEngine(2, 3);
    List<ShufflePartitionedBlock> expectedBlocks1 = Lists.newArrayList();
    List<ShufflePartitionedBlock> expectedBlocks2 = Lists.newArrayList();
    String shuffleFilePath1 = RssUtils.getShuffleDataPath(appId, shuffleId, 0, 1);
    String shuffleFilePath2 = RssUtils.getShuffleDataPath(appId, shuffleId, 2, 3);

    shuffleEngineManager.commit();

    // won't flush for partition 0-1
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
    expectedBlocks1.addAll(partitionedData0.getBlockList());
    shuffleEngineManager.getShuffleEngine(1).write(partitionedData0);

    shuffleEngineManager.commit();
    assertEquals(1, shuffleFlushManager.getEventIds(shuffleFilePath1).size());
    assertNull(shuffleFlushManager.getEventIds(shuffleFilePath2));

    // flush for partition 0-1
    ShufflePartitionedData partitionedData1 = createPartitionedData(0, 2, 35);
    expectedBlocks1.addAll(partitionedData1.getBlockList());
    shuffleEngineManager.getShuffleEngine(0).write(partitionedData1);

    // won't flush for partition 0-1
    ShufflePartitionedData partitionedData2 = createPartitionedData(1, 1, 35);
    expectedBlocks1.addAll(partitionedData2.getBlockList());
    shuffleEngineManager.getShuffleEngine(1).write(partitionedData2);

    // won't flush for partition 2-3
    ShufflePartitionedData partitionedData3 = createPartitionedData(2, 1, 35);
    expectedBlocks2.addAll(partitionedData3.getBlockList());
    shuffleEngineManager.getShuffleEngine(2).write(partitionedData3);

    // flush for partition 2-3
    ShufflePartitionedData partitionedData4 = createPartitionedData(3, 1, 35);
    expectedBlocks2.addAll(partitionedData4.getBlockList());
    shuffleEngineManager.getShuffleEngine(3).write(partitionedData4);

    shuffleEngineManager.commit();
    // 1 event created by flush, 1 event created by commit
    assertEquals(3, shuffleFlushManager.getEventIds(shuffleFilePath1).size());
    assertEquals(2, shuffleFlushManager.getEventIds(shuffleFilePath2).size());

    // flush for partition 0-1
    ShufflePartitionedData partitionedData5 = createPartitionedData(0, 2, 35);
    expectedBlocks1.addAll(partitionedData5.getBlockList());
    shuffleEngineManager.getShuffleEngine(0).write(partitionedData5);

    shuffleEngineManager.commit();
    assertEquals(4, shuffleFlushManager.getEventIds(shuffleFilePath1).size());
    assertEquals(2, shuffleFlushManager.getEventIds(shuffleFilePath2).size());

    shuffleEngineManager.commit();
    assertEquals(4, shuffleFlushManager.getEventIds(shuffleFilePath1).size());
    assertEquals(2, shuffleFlushManager.getEventIds(shuffleFilePath2).size());

    String shuffleDataFolder = RssUtils.getFullShuffleDataFolder(storageBasePath, shuffleFilePath1);
    validate(expectedBlocks1, shuffleDataFolder, serverId);

    shuffleDataFolder = RssUtils.getFullShuffleDataFolder(storageBasePath, shuffleFilePath2);
    validate(expectedBlocks2, shuffleDataFolder, serverId);

    // flush for partition 0-1
    ShufflePartitionedData partitionedData7 = createPartitionedData(0, 2, 35);
    shuffleEngineManager.getShuffleEngine(0).write(partitionedData7);
    int retry = 0;
    while (true) {
      // remove flushed eventId to test timeout in commit
      if (shuffleFlushManager.getEventIds(shuffleFilePath1).size() == 5) {
        shuffleFlushManager.getEventIds(shuffleFilePath1).clear();
        break;
      }
      Thread.sleep(1000);
      retry++;
      if (retry > 5) {
        fail("Timeout to flush data");
      }
    }
    try {
      shuffleEngineManager.commit();
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Shuffle data commit timeout for"));
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

  private void validate(List<ShufflePartitionedBlock> blocks,
      String basePath, String fileNamePrefix) throws Exception {
    FileBasedShuffleReadHandler handler = new FileBasedShuffleReadHandler(basePath, fileNamePrefix, conf);
    List<FileBasedShuffleSegment> allSegments = Lists.newArrayList();
    List<FileBasedShuffleSegment> segments = handler.readIndex(100);
    while (!segments.isEmpty()) {
      allSegments.addAll(segments);
      segments = handler.readIndex(100);
    }
    assertEquals(blocks.size(), allSegments.size());
    int matchNum = 0;
    for (ShufflePartitionedBlock block : blocks) {
      for (FileBasedShuffleSegment segment : allSegments) {
        if (block.getBlockId() == segment.getBlockId()) {
          assertEquals(block.getLength(), segment.getLength());
          assertEquals(block.getCrc(), segment.getCrc());
          matchNum++;
        }
      }
    }
    assertEquals(blocks.size(), matchNum);
  }
}
