package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.RangeMap;
import com.google.common.io.Files;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import java.util.Map;
import java.util.Random;
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
    conf.setString("rss.server.buffer.capacity", "150");
    conf.setString("rss.server.buffer.spill.threshold", "128");
    conf.setString("rss.server.partition.buffer.size", "32");
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

    StatusCode sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);
    shuffleBufferManager.registerBuffer(appId, shuffleId + 1, 0, 1);
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 100, 101);
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_REGISTER, sc);

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.SUCCESS, sc);

    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool = shuffleBufferManager.getBufferPool();
    ShuffleBuffer buffer = bufferPool.get(appId).get(shuffleId).get(0);
    assertEquals(16, buffer.getSize());
    assertEquals(16, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(32, buffer.getSize());
    assertEquals(32, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 1));
    assertEquals(0, buffer.getSize());
    assertEquals(33, shuffleBufferManager.getUsedMemory());
    assertEquals(33, shuffleBufferManager.getInFlushSize());
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 95));
    assertEquals(0, buffer.getSize());
    assertEquals(128, shuffleBufferManager.getUsedMemory());
    assertEquals(128, shuffleBufferManager.getInFlushSize());
    verify(mockShuffleFlushManager, times(2)).addToFlushQueue(any());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 22));
    assertEquals(22, buffer.getSize());
    assertEquals(150, shuffleBufferManager.getUsedMemory());
    verify(mockShuffleFlushManager, times(2)).addToFlushQueue(any());

    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(StatusCode.NO_BUFFER, sc);

    // size won't be reduce which should be processed by flushManager, reset buffer size to 0
    shuffleBufferManager.resetSize();
    shuffleBufferManager.removeBuffer(appId);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 2, 3);
    shuffleBufferManager.registerBuffer(appId, shuffleId, 4, 5);
    shuffleBufferManager.registerBuffer(appId, 2, 0, 1);
    shuffleBufferManager.registerBuffer("appId1", shuffleId, 0, 1);
    shuffleBufferManager.registerBuffer("appId2", shuffleId, 0, 1);

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(2, 33));
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(4, 33));
    shuffleBufferManager.cacheShuffleData(appId, 2, false, createData(0, 32));
    shuffleBufferManager.cacheShuffleData("appId1", shuffleId, false, createData(0, 32));
    assertEquals(130, shuffleBufferManager.getUsedMemory());
    assertEquals(66, shuffleBufferManager.getInFlushSize());
    shuffleBufferManager.cacheShuffleData("appId2", shuffleId, false, createData(0, 33));
    assertEquals(163, shuffleBufferManager.getUsedMemory());
    assertEquals(99, shuffleBufferManager.getInFlushSize());
    verify(mockShuffleFlushManager, times(5)).addToFlushQueue(any());
  }

  @Test
  public void cacheShuffleDataWithPreAllocationTest() {
    String appId = "cacheShuffleDataWithPreAllocationTest";
    int shuffleId = 1;

    shuffleBufferManager.registerBuffer(appId, shuffleId, 0, 1);
    // pre allocate memory
    shuffleBufferManager.requireMemory(16, true);
    assertEquals(16, shuffleBufferManager.getUsedMemory());
    assertEquals(16, shuffleBufferManager.getPreAllocatedSize());
    // receive data with preAllocation
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(0, 16));
    assertEquals(16, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());
    // release memory
    shuffleBufferManager.releaseMemory(16, false);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());
    // receive data without preAllocation
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 17));
    assertEquals(17, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());
    // single buffer flush
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());
    // release memory
    shuffleBufferManager.releaseMemory(17, false);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getPreAllocatedSize());

    // pre allocate all memory
    shuffleBufferManager.requireMemory(150, true);
    assertEquals(150, shuffleBufferManager.getUsedMemory());
    assertEquals(150, shuffleBufferManager.getPreAllocatedSize());

    // no buffer if data without pre allocation
    StatusCode sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(1, 16));
    assertEquals(StatusCode.NO_BUFFER, sc);

    // pre allocation > threshold, but actual data size < threshold / 2, won't flush
    sc = shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(1, 16));
    assertEquals(StatusCode.SUCCESS, sc);
    assertEquals(150, shuffleBufferManager.getUsedMemory());
    assertEquals(134, shuffleBufferManager.getPreAllocatedSize());
    // no flush happen
    verify(mockShuffleFlushManager, times(1)).addToFlushQueue(any());

    // actual data size > threshold / 2, flush
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, true, createData(0, 60));
    assertEquals(StatusCode.SUCCESS, sc);
    assertEquals(150, shuffleBufferManager.getUsedMemory());
    assertEquals(74, shuffleBufferManager.getPreAllocatedSize());
    verify(mockShuffleFlushManager, times(2)).addToFlushQueue(any());
  }

  @Test
  public void bufferSizeTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir = new File(tmpDir, "data");
    conf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    conf.setString("rss.storage.basePath", dataDir.getAbsolutePath());
    conf.setString("rss.server.buffer.capacity", "150");
    conf.setString("rss.server.buffer.spill.threshold", "128");
    conf.setString("rss.server.partition.buffer.size", "32");

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
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(16, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 16));
    assertEquals(32, shuffleBufferManager.getUsedMemory());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 1));
    waitForFlush(shuffleFlushManager, appId, shuffleId, 3);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 32));
    assertEquals(32, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(2, 32));
    assertEquals(64, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(4, 32));
    assertEquals(96, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(6, 32));
    assertEquals(128, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(8, 32));
    waitForFlush(shuffleFlushManager, appId, shuffleId, 5);
    assertEquals(0, shuffleBufferManager.getUsedMemory());
    assertEquals(0, shuffleBufferManager.getInFlushSize());

    shuffleBufferManager.registerBuffer("bufferSizeTest1", shuffleId, 0, 1);
    shuffleBufferManager.cacheShuffleData(appId, shuffleId, false, createData(0, 32));
    assertEquals(32, shuffleBufferManager.getUsedMemory());
    shuffleBufferManager.cacheShuffleData("bufferSizeTest1", shuffleId, false, createData(0, 32));
    assertEquals(64, shuffleBufferManager.getUsedMemory());
    assertEquals(2, shuffleBufferManager.getBufferPool().keySet().size());
    shuffleBufferManager.removeBuffer(appId);
    assertEquals(32, shuffleBufferManager.getUsedMemory());
    assertEquals(1, shuffleBufferManager.getBufferPool().keySet().size());
  }

  private void waitForFlush(ShuffleFlushManager shuffleFlushManager,
      String appId, int shuffleId, int expectedBlockNum) throws Exception {
    int retry = 0;
    int committedCount = 0;
    do {
      committedCount = shuffleFlushManager.getCommittedBlockCount(appId, shuffleId);
      if (committedCount < expectedBlockNum) {
        Thread.sleep(500);
      }
      retry++;
      if (retry > 10) {
        fail("Flush data time out");
      }
    } while (committedCount < expectedBlockNum);
  }

  private ShufflePartitionedData createData(int partitionId, int len) {
    byte[] buf = new byte[len];
    new Random().nextBytes(buf);
    ShufflePartitionedBlock block = new ShufflePartitionedBlock(len, len, 1, 1, buf);
    ShufflePartitionedData data = new ShufflePartitionedData(partitionId, block);
    return data;
  }
}
