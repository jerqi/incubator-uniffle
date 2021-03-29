package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.impl.HdfsClientReadHandler;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.Test;

public class ShuffleFlushManagerTest extends HdfsTestBase {

  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);
  private static AtomicLong ATOMIC_LONG = new AtomicLong(0);

  static {
    ShuffleServerMetrics.register();
  }

  private ShuffleServerConf shuffleServerConf = new ShuffleServerConf();
  private String storageBasePath = HDFS_URI + "rss/test";

  @Before
  public void prepare() {
    shuffleServerConf.setString("rss.storage.basePath", storageBasePath);
    shuffleServerConf.setString("rss.storage.type", "HDFS");
    LogManager.getRootLogger().setLevel(Level.INFO);
  }

  @Test
  public void writeTest() throws Exception {
    ShuffleFlushManager manager =
        new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null);
    ShuffleDataFlushEvent event1 =
        createShuffleDataFlushEvent("appId1", 1, 1, 1);
    List<ShufflePartitionedBlock> blocks1 = event1.getShuffleBlocks();
    manager.addToFlushQueue(event1);
    ShuffleDataFlushEvent event21 =
        createShuffleDataFlushEvent("appId1", 2, 2, 2);
    List<ShufflePartitionedBlock> blocks21 = event21.getShuffleBlocks();
    manager.addToFlushQueue(event21);
    ShuffleDataFlushEvent event22 =
        createShuffleDataFlushEvent("appId1", 2, 2, 2);
    List<ShufflePartitionedBlock> blocks22 = event22.getShuffleBlocks();
    manager.addToFlushQueue(event22);
    // wait for write data
    waitForFlush(manager, "appId1", 1, 5);
    waitForFlush(manager, "appId1", 2, 10);
    validate("appId1", 1, 1, blocks1, 1, storageBasePath);
    assertEquals(blocks1.size(), manager.getCommittedBlockCount("appId1", 1));

    blocks21.addAll(blocks22);
    validate("appId1", 2, 2, blocks21, 1, storageBasePath);
    assertEquals(blocks21.size(), manager.getCommittedBlockCount("appId1", 2));
  }

  @Test
  public void complexWriteTest() throws Exception {
    shuffleServerConf.setString("rss.server.flush.handler.expired", "3");
    shuffleServerConf.setString("rss.server.flush.gc.check.interval", "1");
    List<ShufflePartitionedBlock> expectedBlocks = Lists.newArrayList();
    List<ShuffleDataFlushEvent> flushEvents1 = Lists.newArrayList();
    List<ShuffleDataFlushEvent> flushEvents2 = Lists.newArrayList();
    ShuffleFlushManager manager = new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null);
    for (int i = 0; i < 30; i++) {
      ShuffleDataFlushEvent flushEvent1 = createShuffleDataFlushEvent("appId4", 1, 1, 1);
      ShuffleDataFlushEvent flushEvent2 = createShuffleDataFlushEvent("appId4", 1, 1, 1);
      expectedBlocks.addAll(flushEvent1.getShuffleBlocks());
      expectedBlocks.addAll(flushEvent2.getShuffleBlocks());
      flushEvents1.add(flushEvent1);
      flushEvents2.add(flushEvent2);
    }
    Thread flushThread1 = new Thread(() -> {
      for (ShuffleDataFlushEvent event : flushEvents1) {
        manager.addToFlushQueue(event);
      }
    });

    Thread flushThread2 = new Thread(() -> {
      for (ShuffleDataFlushEvent event : flushEvents2) {
        manager.addToFlushQueue(event);
      }
    });
    flushThread1.start();
    flushThread2.start();
    flushThread1.join();
    flushThread2.join();

    waitForFlush(manager, "appId4", 1, 300);
    validate("appId4", 1, 1, expectedBlocks, 1, storageBasePath);
  }

  @Test
  public void clearTest() throws Exception {
    ShuffleFlushManager manager =
        new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null);
    ShuffleDataFlushEvent event1 =
        createShuffleDataFlushEvent("appId1", 1, 0, 1);
    manager.addToFlushQueue(event1);
    ShuffleDataFlushEvent event2 =
        createShuffleDataFlushEvent("appId2", 1, 0, 1);
    manager.addToFlushQueue(event2);
    waitForFlush(manager, "appId1", 1, 5);
    waitForFlush(manager, "appId2", 1, 5);
    assertEquals(5, manager.getCommittedBlockCount("appId1", 1));
    assertEquals(5, manager.getCommittedBlockCount("appId2", 1));
    assertEquals(2, manager.getHandlers().size());
    manager.removeResources("appId1");
    assertEquals(0, manager.getCommittedBlockCount("appId1", 1));
    assertEquals(5, manager.getCommittedBlockCount("appId2", 1));
    assertEquals(1, manager.getHandlers().size());
    manager.removeResources("appId2");
    assertEquals(0, manager.getCommittedBlockCount("appId2", 1));
    assertEquals(0, manager.getHandlers().size());
  }

  private void waitForFlush(ShuffleFlushManager manager,
      String appId, int shuffleId, int expectedBlockNum) throws Exception {
    int retry = 0;
    int size = 0;
    do {
      Thread.sleep(500);
      if (retry > 100) {
        fail("Unexpected flush process");
      }
      retry++;
      size = manager.getCommittedBlockCount(appId, shuffleId);
    } while (size < expectedBlockNum);
  }

  private ShuffleDataFlushEvent createShuffleDataFlushEvent(
      String appId, int shuffleId, int startPartition, int endPartition) {
    List<ShufflePartitionedBlock> spbs = createBlock(5, 32);
    return new ShuffleDataFlushEvent(ATOMIC_LONG.getAndIncrement(),
        appId, shuffleId, startPartition, endPartition, 1, spbs);
  }

  private List<ShufflePartitionedBlock> createBlock(int num, int length) {
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
    for (int i = 0; i < num; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      blocks.add(new ShufflePartitionedBlock(
          length, length, ChecksumUtils.getCrc32(buf), ATOMIC_INT.incrementAndGet(), buf));
    }
    return blocks;
  }

  private void validate(String appId, int shuffleId, int partitionId, List<ShufflePartitionedBlock> blocks,
      int partitionNumPerRange, String basePath) {
    Set<Long> blockIds = Sets.newHashSet(blocks.stream().map(spb -> spb.getBlockId()).collect(Collectors.toList()));
    HdfsClientReadHandler handler = new HdfsClientReadHandler(appId, shuffleId, partitionId,
        100, partitionNumPerRange, 10, blocks.size() * 32, basePath, blockIds);
    ShuffleDataResult sdr = null;
    int matchNum = 0;
    Set<Long> remainIds = Sets.newHashSet(blockIds);
    sdr = handler.readShuffleData(remainIds);
    List<BufferSegment> bufferSegments = sdr.getBufferSegments();
    for (ShufflePartitionedBlock block : blocks) {
      for (BufferSegment bs : bufferSegments) {
        if (bs.getBlockId() == block.getBlockId()) {
          matchNum++;
          break;
        }
      }
    }
    for (BufferSegment bs : bufferSegments) {
      remainIds.remove(bs.getBlockId());
    }
    assertEquals(blocks.size(), matchNum);
  }
}
