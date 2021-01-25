package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.FileBasedShuffleReadHandler;
import com.tencent.rss.storage.FileBasedShuffleSegment;
import com.tencent.rss.storage.HdfsTestBase;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.Test;

public class ShuffleFlushManagerTest extends HdfsTestBase {

  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);
  private static AtomicLong ATOMIC_LONG = new AtomicLong(0);
  private ShuffleServerConf shuffleServerConf = new ShuffleServerConf();
  private String storageBasePath = HDFS_URI + "rss/test";

  @Before
  public void prepare() {
    shuffleServerConf.setString("rss.storage.basePath", storageBasePath);
    LogManager.getRootLogger().setLevel(Level.INFO);
  }

  @Test
  public void testWrite() throws Exception {
    ShuffleFlushManager manager =
        new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null);
    ShuffleDataFlushEvent event1 =
        createShuffleDataFlushEvent("appId1", "1", 0, 1);
    List<ShufflePartitionedBlock> blocks1 = event1.getShuffleBlocks();
    manager.addToFlushQueue(event1);
    ShuffleDataFlushEvent event21 =
        createShuffleDataFlushEvent("appId1", "2", 0, 1);
    List<ShufflePartitionedBlock> blocks21 = event21.getShuffleBlocks();
    manager.addToFlushQueue(event21);
    ShuffleDataFlushEvent event22 =
        createShuffleDataFlushEvent("appId1", "2", 0, 1);
    List<ShufflePartitionedBlock> blocks22 = event22.getShuffleBlocks();
    manager.addToFlushQueue(event22);
    // wait for write data
    Thread.sleep(5000);
    String shuffleDataFolder = RssUtils.getFullShuffleDataFolder(storageBasePath, event1.getShuffleFilePath());
    validate(blocks1, shuffleDataFolder, "shuffleServerId");
    assertEquals(1, manager.getEventIds(event1.getShuffleFilePath()).size());

    shuffleDataFolder = RssUtils.getFullShuffleDataFolder(storageBasePath, event21.getShuffleFilePath());
    blocks21.addAll(blocks22);
    validate(blocks21, shuffleDataFolder, "shuffleServerId");
    assertEquals(2, manager.getEventIds(event21.getShuffleFilePath()).size());
  }

  @Test
  public void testGC() throws Exception {
    shuffleServerConf.setString("rss.server.flush.handler.expired", "5");
    shuffleServerConf.setString("rss.server.flush.gc.check.interval", "1");
    ShuffleFlushManager manager = new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null);
    ShuffleDataFlushEvent event1 = createShuffleDataFlushEvent("appId3", "1", 0, 1);
    manager.addToFlushQueue(event1);
    ShuffleDataFlushEvent event2 = createShuffleDataFlushEvent("appId3", "2", 0, 1);
    manager.addToFlushQueue(event2);
    Thread.sleep(4000);
    assertEquals(2, manager.getPathToHandler().size());
    assertEquals(1, manager.getEventIds(event1.getShuffleFilePath()).size());
    assertEquals(1, manager.getEventIds(event2.getShuffleFilePath()).size());
    event2 = createShuffleDataFlushEvent("appId3", "2", 0, 1);
    manager.addToFlushQueue(event2);
    Thread.sleep(4000);
    assertEquals(1, manager.getPathToHandler().size());
    assertNull(manager.getEventIds(event1.getShuffleFilePath()));
    assertEquals(2, manager.getEventIds(event2.getShuffleFilePath()).size());
    Thread.sleep(4000);
    assertEquals(0, manager.getPathToHandler().size());
    assertNull(manager.getEventIds(event1.getShuffleFilePath()));
    assertNull(manager.getEventIds(event2.getShuffleFilePath()));
  }

  @Test
  public void testComplexWrite() throws Exception {
    shuffleServerConf.setString("rss.server.flush.handler.expired", "3");
    shuffleServerConf.setString("rss.server.flush.gc.check.interval", "1");
    List<ShufflePartitionedBlock> expectedBlocks = Lists.newArrayList();
    List<ShuffleDataFlushEvent> flushEvents1 = Lists.newArrayList();
    List<ShuffleDataFlushEvent> flushEvents2 = Lists.newArrayList();
    ShuffleFlushManager manager = new ShuffleFlushManager(shuffleServerConf, "shuffleServerId", null);
    String shuffleFilePath = "";
    for (int i = 0; i < 30; i++) {
      ShuffleDataFlushEvent flushEvent1 = createShuffleDataFlushEvent("appId4", "1", 0, 1);
      ShuffleDataFlushEvent flushEvent2 = createShuffleDataFlushEvent("appId4", "1", 0, 1);
      expectedBlocks.addAll(flushEvent1.getShuffleBlocks());
      expectedBlocks.addAll(flushEvent2.getShuffleBlocks());
      flushEvents1.add(flushEvent1);
      flushEvents2.add(flushEvent2);
      if ("".equals(shuffleFilePath)) {
        shuffleFilePath = flushEvent1.getShuffleFilePath();
      }
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

    int retryNum = 0;
    // wait for flush thread
    while (true) {
      if (manager.getEventIds(shuffleFilePath) != null
          && manager.getEventIds(shuffleFilePath).size() == 60) {
        break;
      }
      Thread.sleep(1000);
      retryNum++;
      if (retryNum > 120) {
        throw new RuntimeException("Failed to pass test");
      }
    }

    String shuffleDataFolder = RssUtils.getFullShuffleDataFolder(storageBasePath, shuffleFilePath);
    validate(expectedBlocks, shuffleDataFolder, "shuffleServerId");
  }

  private ShuffleDataFlushEvent createShuffleDataFlushEvent(
      String appId, String shuffleId, int startPartition, int endPartition) {
    List<ShufflePartitionedBlock> spbs = createBlock(5, 32);
    return new ShuffleDataFlushEvent(ATOMIC_LONG.getAndIncrement(),
        appId, shuffleId, startPartition, endPartition, -1, spbs);
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
