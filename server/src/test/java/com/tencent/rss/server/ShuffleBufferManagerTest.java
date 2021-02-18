package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class ShuffleBufferManagerTest {

  static {
    ShuffleServerMetrics.register();
  }

  private ShuffleBufferManager shuffleBufferManager;
  private ShuffleFlushManager mockShuffleFlushManager;

  @Before
  public void setUp() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setString("rss.server.buffer.capacity", "128");
    conf.setString("rss.server.buffer.size", "32");
    mockShuffleFlushManager = mock(ShuffleFlushManager.class);
    shuffleBufferManager = new ShuffleBufferManager(conf, mockShuffleFlushManager);
  }

  @Test
  public void registerBufferTest() {
    String appId = "registerBufferTest";
    int shuffleId = 1;

    StatusCode sc = shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    assertEquals(StatusCode.SUCCESS, sc);
    sc = shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    assertEquals(StatusCode.SUCCESS, sc);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool = shuffleBufferManager.getBufferPool();

    assertNotNull(bufferPool.get(appId).get(shuffleId).get(0));
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(1));
    assertNotNull(bufferPool.get(appId).get(shuffleId).get(2));
    assertEquals(bufferPool.get(appId).get(shuffleId).get(2), bufferPool.get(appId).get(shuffleId).get(3));

    // register again
    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    assertEquals(buffer, bufferPool.get(appId).get(shuffleId).get(0));
  }

  @Test
  public void cacheShuffleDataTest() {
    String appId = "cacheShuffleDataTest";
    int shuffleId = 1;

    StatusCode sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);
    shuffleBufferManager.registerBuffer(appId, shuffleId + 1, 0, 1);
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 100, 101);
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 16));
    assertEquals(StatusCode.SUCCESS, sc);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool = shuffleBufferManager.getBufferPool();
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(16, buffer.getSize());
    assertEquals(16, shuffleBufferManager.getSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 16));
    assertEquals(32, buffer.getSize());
    assertEquals(32, shuffleBufferManager.getSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 1));
    assertEquals(0, buffer.getSize());
    assertEquals(33, shuffleBufferManager.getSize());
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 95));
    assertEquals(0, buffer.getSize());
    assertEquals(128, shuffleBufferManager.getSize());
    verify(mockShuffleFlushManager, times(2)).addToFlushQueue(any());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 1));
    assertEquals(0, buffer.getSize());
    assertEquals(129, shuffleBufferManager.getSize());
    verify(mockShuffleFlushManager, times(3)).addToFlushQueue(any());

    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 16));
    assertEquals(StatusCode.NO_BUFFER, sc);

    // size won't be reduce which should be processed by flushManager, reset buffer size to 0
    shuffleBufferManager.resetSize();
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.registerBuffer(appId, 2, 0, 1);
    shuffleBufferManager.registerBuffer("appId1", shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer("appId2", shuffleId, 0, 1);

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 32));
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(2, 32));
    shuffleBufferManager.cacheShuffleData(appId, 2, createData(0, 32));
    shuffleBufferManager.cacheShuffleData("appId1", shuffleId, createData(0, 32));
    assertEquals(128, shuffleBufferManager.getSize());
    shuffleBufferManager.cacheShuffleData("appId2", shuffleId, createData(0, 32));
    assertEquals(160, shuffleBufferManager.getSize());
    verify(mockShuffleFlushManager, times(8)).addToFlushQueue(any());
  }

  @Test
  public void commitShuffleTaskTest() {
    String appId = "commitShuffleTaskTest";
    int shuffleId = 1;

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.registerBuffer(appId, 2, 0, 1);
    RangeMap<Integer, Set<Long>> rangeMap = shuffleBufferManager.commitShuffleTask(appId, shuffleId);
    assertEquals(0, rangeMap.asMapOfRanges().size());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 32));
    rangeMap = shuffleBufferManager.commitShuffleTask(appId, shuffleId);
    assertEquals(1, rangeMap.get(0).size());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 32));
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(2, 32));
    rangeMap = shuffleBufferManager.commitShuffleTask(appId, shuffleId);
    assertEquals(1, rangeMap.get(0).size());
    assertEquals(1, rangeMap.get(2).size());

    // size won't be reduce which should be processed by flushManager, reset buffer size to 0
    shuffleBufferManager.resetSize();
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 32));
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(2, 32));
    shuffleBufferManager.cacheShuffleData(appId, 2, createData(0, 32));
    rangeMap = shuffleBufferManager.commitShuffleTask(appId, shuffleId);
    Map<Range<Integer>, Set<Long>> map = rangeMap.asMapOfRanges();
    assertEquals(2, map.size());
    assertEquals(1, map.get(Range.closed(0, 1)).size());
    assertEquals(1, map.get(Range.closed(2, 3)).size());
  }

  @Test
  public void bufferSizeTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir = new File(tmpDir, "data");
    conf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    conf.setString("rss.storage.basePath", dataDir.getAbsolutePath());
    conf.setString("rss.server.buffer.capacity", "128");
    conf.setString("rss.server.buffer.size", "32");

    ShuffleServer mockShuffleServer = mock(ShuffleServer.class);
    ShuffleFlushManager shuffleFlushManager = new ShuffleFlushManager(conf, "serverId", mockShuffleServer);
    shuffleBufferManager = new ShuffleBufferManager(conf, shuffleFlushManager);

    when(mockShuffleServer
        .getShuffleFlushManager())
        .thenReturn(shuffleFlushManager);
    when(mockShuffleServer
        .getShuffleBufferManager())
        .thenReturn(shuffleBufferManager);

    String appId = "bufferSizeTest";
    int shuffleId = 1;

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 4, 5);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 6, 7);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 8, 9);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 16));
    assertEquals(16, shuffleBufferManager.getSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 16));
    assertEquals(32, shuffleBufferManager.getSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 1));
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(0, 1), 1);
    assertEquals(0, shuffleBufferManager.getSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(0, 32));
    assertEquals(32, shuffleBufferManager.getSize());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(2, 32));
    assertEquals(64, shuffleBufferManager.getSize());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(4, 32));
    assertEquals(96, shuffleBufferManager.getSize());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(6, 32));
    assertEquals(128, shuffleBufferManager.getSize());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, createData(8, 32));
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(0, 1), 2);
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(2, 3), 1);
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(4, 5), 1);
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(6, 7), 1);
    waitForFlush(shuffleFlushManager, appId, shuffleId, Range.closed(8, 9), 1);
    assertEquals(0, shuffleBufferManager.getSize());
  }

  private void waitForFlush(ShuffleFlushManager shuffleFlushManager,
      String appId, int shuffleId, Range<Integer> range, int expectedNum) throws Exception {
    Set<Long> eventIds = Sets.newHashSet();
    int retry = 0;
    do {
      eventIds = shuffleFlushManager.getEventIds(appId, shuffleId, range);
      if (eventIds.size() < expectedNum) {
        Thread.sleep(1000);
      }
      retry++;
      if (retry > 10) {
        fail("Flush data time out");
      }
    } while (eventIds.size() < expectedNum);
  }

  private ShufflePartitionedData createData(int partitionId, int len) {
    byte[] buf = new byte[len];
    new Random().nextBytes(buf);
    ShufflePartitionedBlock block = new ShufflePartitionedBlock(len, 1, 1, buf);
    ShufflePartitionedData data = new ShufflePartitionedData(partitionId, block);
    return data;
  }
}
