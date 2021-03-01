package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.impl.HdfsClientReadHandler;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;

public class ShuffleTaskManagerTest extends HdfsTestBase {

  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);
  private ShuffleTaskManager shuffleTaskManager;
  private ShuffleServer shuffleServer;

  @Before
  public void setUp() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
    ShuffleServerConf conf = new ShuffleServerConf(confFile);
    String storageBasePath = HDFS_URI + "rss/test";
    conf.setString("rss.server.buffer.capacity", "64");
    conf.setString("rss.server.buffer.size", "64");
    conf.setString("rss.storage.basePath", storageBasePath);
    conf.setString("rss.storage.type", "HDFS");
    conf.setString("rss.server.commit.timeout", "10000");
    shuffleServer = new ShuffleServer(conf);
    shuffleTaskManager = new ShuffleTaskManager(conf,
        shuffleServer.getShuffleFlushManager(), shuffleServer.getShuffleBufferManager());
  }

  @Test
  public void registerShuffleTest() {
    String appId = "registerTest1";
    int shuffleId = 1;

    shuffleTaskManager.registerShuffle(appId, shuffleId, 0, 1);
    shuffleTaskManager.registerShuffle(appId, shuffleId, 2, 3);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool =
        shuffleServer.getShuffleBufferManager().getBufferPool();

    assertNotNull(bufferPool.get(appId).get(shuffleId).get(0));
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(1));
    assertNotNull(bufferPool.get(appId).get(shuffleId).get(2));
    assertEquals(bufferPool.get(appId).get(shuffleId).get(2), bufferPool.get(appId).get(shuffleId).get(3));

    // register again
    shuffleTaskManager.registerShuffle(appId, shuffleId, 0, 1);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(0));
  }

  @Test
  public void writeProcessTest() throws Exception {
    String confFile = ClassLoader.getSystemResource("server.conf").getFile();
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
    ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    ShuffleTaskManager shuffleTaskManager = new ShuffleTaskManager(conf, shuffleFlushManager, shuffleBufferManager);
    shuffleTaskManager.registerShuffle(appId, shuffleId, 0, 1);
    shuffleTaskManager.registerShuffle(appId, shuffleId, 2, 3);
    List<ShufflePartitionedBlock> expectedBlocks0 = Lists.newArrayList();
    List<ShufflePartitionedBlock> expectedBlocks1 = Lists.newArrayList();
    List<ShufflePartitionedBlock> expectedBlocks2 = Lists.newArrayList();
    List<ShufflePartitionedBlock> expectedBlocks3 = Lists.newArrayList();

    shuffleTaskManager.commitShuffle(appId, shuffleId);

    // won't flush for partition 0-1
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);
    expectedBlocks1.addAll(partitionedData0.getBlockList());
    StatusCode sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, partitionedData0);
    assertEquals(StatusCode.SUCCESS, sc);

    shuffleTaskManager.commitShuffle(appId, shuffleId);
    assertEquals(1, shuffleFlushManager.getEventIds(appId, shuffleId, Range.closed(0, 1)).size());
    assertNull(shuffleFlushManager.getEventIds(appId, shuffleId, Range.closed(2, 3)));

    // flush for partition 0-1
    ShufflePartitionedData partitionedData1 = createPartitionedData(0, 2, 35);
    expectedBlocks0.addAll(partitionedData1.getBlockList());
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, partitionedData1);
    assertEquals(StatusCode.SUCCESS, sc);
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(0, 1), 2, false);

    // won't flush for partition 0-1
    ShufflePartitionedData partitionedData2 = createPartitionedData(1, 1, 30);
    expectedBlocks1.addAll(partitionedData2.getBlockList());
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, partitionedData2);
    assertEquals(StatusCode.SUCCESS, sc);

    // won't flush for partition 2-3
    ShufflePartitionedData partitionedData3 = createPartitionedData(2, 1, 30);
    expectedBlocks2.addAll(partitionedData3.getBlockList());
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, partitionedData3);
    assertEquals(StatusCode.SUCCESS, sc);

    // flush for partition 2-3
    ShufflePartitionedData partitionedData4 = createPartitionedData(3, 1, 35);
    expectedBlocks3.addAll(partitionedData4.getBlockList());
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, partitionedData4);
    assertEquals(StatusCode.SUCCESS, sc);

    shuffleTaskManager.commitShuffle(appId, shuffleId);
    // 1 event created by flush, 1 event created by commit
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(0, 1), 3, false);
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(2, 3), 1, false);
    assertEquals(3, shuffleFlushManager.getEventIds(appId, shuffleId, Range.closed(0, 1)).size());
    assertEquals(1, shuffleFlushManager.getEventIds(appId, shuffleId, Range.closed(2, 3)).size());

    // flush for partition 0-1
    ShufflePartitionedData partitionedData5 = createPartitionedData(0, 2, 35);
    expectedBlocks1.addAll(partitionedData5.getBlockList());
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, partitionedData5);
    assertEquals(StatusCode.SUCCESS, sc);

    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(0, 1), 4, false);
    shuffleTaskManager.commitShuffle(appId, shuffleId);
    assertEquals(4, shuffleFlushManager.getEventIds(appId, shuffleId, Range.closed(0, 1)).size());
    assertEquals(1, shuffleFlushManager.getEventIds(appId, shuffleId, Range.closed(2, 3)).size());

    shuffleTaskManager.commitShuffle(appId, shuffleId);
    assertEquals(4, shuffleFlushManager.getEventIds(appId, shuffleId, Range.closed(0, 1)).size());
    assertEquals(1, shuffleFlushManager.getEventIds(appId, shuffleId, Range.closed(2, 3)).size());

    validate(appId, shuffleId, 0, expectedBlocks0, storageBasePath);
    validate(appId, shuffleId, 1, expectedBlocks1, storageBasePath);
    validate(appId, shuffleId, 2, expectedBlocks2, storageBasePath);
    validate(appId, shuffleId, 3, expectedBlocks3, storageBasePath);

    // flush for partition 0-1
    ShufflePartitionedData partitionedData7 = createPartitionedData(0, 2, 35);
    sc = shuffleTaskManager.cacheShuffleData(appId, shuffleId, partitionedData7);
    assertEquals(StatusCode.SUCCESS, sc);

    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(0, 1), 5, true);
    try {
      shuffleTaskManager.commitShuffle(appId, shuffleId);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Shuffle data commit timeout for"));
    }
  }

  @Test
  public void clearTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    String storageBasePath = HDFS_URI + "rss/clearTest";
    int shuffleId = 1;
    conf.setString("rss.rpc.server.port", "1234");
    conf.setString("rss.server.coordinator.ip", "localhost");
    conf.setString("rss.server.coordinator.port", "9527");
    conf.setString("rss.jetty.http.port", "12345");
    conf.setString("rss.jetty.corePool.size", "64");
    conf.setString("rss.server.buffer.capacity", "64");
    conf.setString("rss.server.buffer.size", "64");
    conf.setString("rss.storage.basePath", storageBasePath);
    conf.setString("rss.storage.type", "HDFS");
    conf.setString("rss.server.commit.timeout", "10000");
    conf.setString("rss.server.app.expired.withHeartbeat", "5000");
    conf.setString("rss.server.app.expired.withoutHeartbeat", "2000");
    ShuffleServer shuffleServer = new ShuffleServer(conf);
    ShuffleBufferManager shuffleBufferManager = shuffleServer.getShuffleBufferManager();
    ShuffleFlushManager shuffleFlushManager = shuffleServer.getShuffleFlushManager();
    ShuffleTaskManager shuffleTaskManager = new ShuffleTaskManager(conf, shuffleFlushManager, shuffleBufferManager);
    shuffleTaskManager.registerShuffle("clearTest1", shuffleId, 0, 1);
    shuffleTaskManager.registerShuffle("clearTest2", shuffleId, 0, 1);
    assertEquals(2, shuffleTaskManager.getAppIds().size());
    ShufflePartitionedData partitionedData0 = createPartitionedData(1, 1, 35);

    // keep refresh status of application "clearTest1"
    int retry = 0;
    while (retry < 10) {
      Thread.sleep(1000);
      shuffleTaskManager.cacheShuffleData("clearTest1", shuffleId, partitionedData0);
      shuffleTaskManager.checkResourceStatus(Sets.newHashSet("clearTest1", "clearTest2"));
      retry++;
    }
    // application "clearTest2" was removed according to rss.server.app.expired.withHeartbeat
    assertEquals(Sets.newHashSet("clearTest1"), shuffleTaskManager.getAppIds().keySet());

    // register again
    shuffleTaskManager.registerShuffle("clearTest2", shuffleId, 0, 1);
    shuffleTaskManager.checkResourceStatus(Sets.newHashSet("clearTest1"));
    assertEquals(Sets.newHashSet("clearTest1", "clearTest2"), shuffleTaskManager.getAppIds().keySet());
    Thread.sleep(3000);
    // application "clearTest2" was removed according to rss.server.app.expired.withoutHeartbeat
    shuffleTaskManager.checkResourceStatus(Sets.newHashSet("clearTest1"));
    assertEquals(Sets.newHashSet("clearTest1"), shuffleTaskManager.getAppIds().keySet());
  }

  private void waitForFlush(ShuffleFlushManager shuffleFlushManager,
      String appId, int shuffleId, Range<Integer> range, int eventNum, boolean isClear) throws Exception {
    int retry = 0;
    while (true) {
      // remove flushed eventId to test timeout in commit
      if (shuffleFlushManager.getEventIds(appId, shuffleId, range).size() == eventNum) {
        if (isClear) {
          shuffleFlushManager.getEventIds(appId, shuffleId, range).clear();
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
