package org.apache.spark.shuffle.writer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Maps;
import com.tencent.rss.common.ShuffleBlockInfo;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssClientConfig;
import org.junit.Test;

public class WriteBufferManagerTest {

  private WriteBufferManager createManager(SparkConf conf) {
    Serializer kryoSerializer = new KryoSerializer(conf);
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager wbm = new WriteBufferManager(
        0, 0, bufferOptions, kryoSerializer,
        Maps.newHashMap(), mockTaskMemoryManager, new ShuffleWriteMetrics());
    WriteBufferManager spyManager = spy(wbm);
    doReturn(512L).when(spyManager).acquireMemory(anyLong());
    return spyManager;
  }

  private SparkConf getConf() {
    SparkConf conf = new SparkConf(false);
    conf.set("spark.rss.writer.buffer.size", "128")
        .set("spark.rss.writer.serializer.buffer.size", "256")
        .set("spark.rss.writer.serializer.buffer.max.size", "512")
        .set("spark.rss.writer.pre.allocated.buffer.size", "512")
        .set("spark.rss.writer.buffer.spill.size", "384");
    return conf;
  }

  @Test
  public void addRecordTest() {
    SparkConf conf = getConf();
    conf.set(RssClientConfig.RSS_WRITER_SAMPLE_GROW_RATE, "1.5");
    WriteBufferManager wbm = createManager(conf);
    wbm.setShuffleWriteMetrics(new ShuffleWriteMetrics());
    String testKey = "Key";
    String testValue = "Value";
    List<ShuffleBlockInfo> result = wbm.addRecord(0, testKey, testValue);
    // single buffer is not full, there is no data return
    assertEquals(0, result.size());
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(104, wbm.getUsedBytes());
    assertEquals(0, wbm.getInSendListBytes());
    assertEquals(1, wbm.getBuffers().size());
    result = wbm.addRecord(0, testKey, testValue);
    // single buffer is full
    assertEquals(1, result.size());
    assertEquals(512, wbm.getAllocatedBytes());
    assertEquals(208, wbm.getUsedBytes());
    assertEquals(208, wbm.getInSendListBytes());
    assertEquals(0, wbm.getBuffers().size());
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(2, testKey, testValue);
    // single buffer is not full, and less than spill size
    assertEquals(1024, wbm.getAllocatedBytes());
    assertEquals(520, wbm.getUsedBytes());
    assertEquals(208, wbm.getInSendListBytes());
    assertEquals(3, wbm.getBuffers().size());
    // free memory
    wbm.freeAllocatedMemory(104);
    assertEquals(920, wbm.getAllocatedBytes());
    assertEquals(416, wbm.getUsedBytes());
    assertEquals(104, wbm.getInSendListBytes());

    assertEquals(5, wbm.getShuffleWriteMetrics().recordsWritten());
    assertTrue(wbm.getShuffleWriteMetrics().bytesWritten() > 0);

    // kv > buffer size, but the estimated size from samples is about 104, the single buffer won't flush
    // it's the case which maybe cause memory leak
    result = wbm.addRecord(3, "testKey12345678901testKey1234567890100000",
        "testValue12345678901testKey123456789010000000");
    assertEquals(4, result.size());
    assertEquals(920, wbm.getAllocatedBytes());
    assertEquals(520, wbm.getUsedBytes());
    assertEquals(520, wbm.getInSendListBytes());

    wbm.freeAllocatedMemory(520);
    wbm.addRecord(0, testKey, testValue);
    wbm.addRecord(1, testKey, testValue);
    wbm.addRecord(2, testKey, testValue);
    result = wbm.clear();
    assertEquals(3, result.size());
    assertEquals(400, wbm.getAllocatedBytes());
    assertEquals(312, wbm.getUsedBytes());
    assertEquals(312, wbm.getInSendListBytes());
  }

  @Test
  public void sampleTest() {
    SparkConf conf = getConf();
    // every record will be treated as sample to calculate kv size
    conf.set(RssClientConfig.RSS_WRITER_SAMPLE_GROW_RATE, "1.0");
    WriteBufferManager wbm = createManager(conf);
    String testKey = "test";
    String testValue = "testValue";
    for (int i = 0; i < 100; i++) {
      wbm.addRecord(0, testKey, testValue);
    }
    assertEquals(100, wbm.getSampleNum());
    // 30 records will be treated as sample if rate = 1.1
    conf.set(RssClientConfig.RSS_WRITER_SAMPLE_GROW_RATE, "1.1");
    wbm = createManager(conf);
    for (int i = 0; i < 100; i++) {
      wbm.addRecord(0, testKey, testValue);
    }
    assertEquals(30, wbm.getSampleNum());
  }
}
