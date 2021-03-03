package org.apache.spark.shuffle.writer;

import static org.junit.Assert.assertEquals;
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
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WriteBufferManagerTest {

  private static WriteBufferManager MANAGER_WITH_KRYO_SER;
  private static WriteBufferManager MANAGER_WITH_JAVA_SER;

  @BeforeClass
  public static void init() {
    SparkConf conf = new SparkConf(false);
    conf.set("spark.rss.writer.buffer.size", "32")
        .set("spark.rss.writer.serializer.buffer.size", "32")
        .set("spark.rss.writer.serializer.buffer.max.size", "64")
        .set("spark.rss.writer.buffer.spill.size", "128");
    Serializer kryoSerializer = new KryoSerializer(conf);
    Serializer javaSerializer = new JavaSerializer(conf);
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager wbm1 = new WriteBufferManager(
        0, 0, bufferOptions, kryoSerializer,
        Maps.newHashMap(), mockTaskMemoryManager, new ShuffleWriteMetrics());
    MANAGER_WITH_KRYO_SER = spy(wbm1);
    doReturn(1000000L).when(MANAGER_WITH_KRYO_SER).acquireMemory(anyLong());

    WriteBufferManager wbm2 = new WriteBufferManager(
        0, 0, bufferOptions, javaSerializer,
        Maps.newHashMap(), mockTaskMemoryManager, new ShuffleWriteMetrics());
    MANAGER_WITH_JAVA_SER = spy(wbm2);
    doReturn(1000000L).when(MANAGER_WITH_JAVA_SER).acquireMemory(anyLong());
  }

  @Before
  public void clearBuffer() {
    MANAGER_WITH_KRYO_SER.clear();
    MANAGER_WITH_JAVA_SER.clear();
  }

  @Test
  public void addRecordTest1() {
    MANAGER_WITH_KRYO_SER.setShuffleWriteMetrics(new ShuffleWriteMetrics());
    // after serialized: key + value = 20 byte, after compression, 22b
    String testKey = "Key";
    String testValue = "Value";
    List<ShuffleBlockInfo> result = MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
    // single buffer is not full, there is no data return
    assertEquals(0, result.size());
    assertEquals(64, MANAGER_WITH_KRYO_SER.getAllocatedBytes());
    assertEquals(1, MANAGER_WITH_KRYO_SER.getBuffers().size());
    result = MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
    // single buffer is not full, there is no data return
    assertEquals(0, result.size());
    assertEquals(64, MANAGER_WITH_KRYO_SER.getAllocatedBytes());
    assertEquals(1, MANAGER_WITH_KRYO_SER.getBuffers().size());
    result = MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
    // single buffer is full
    assertEquals(1, result.size());
    assertEquals(0, MANAGER_WITH_KRYO_SER.getAllocatedBytes());
    assertEquals(0, MANAGER_WITH_KRYO_SER.getBuffers().size());
    result.stream().forEach(e -> assertEquals(22, e.getData().length));

    assertEquals(3, MANAGER_WITH_KRYO_SER.getShuffleWriteMetrics().recordsWritten());
    assertEquals(22, MANAGER_WITH_KRYO_SER.getShuffleWriteMetrics().bytesWritten());
  }

  @Test
  public void addRecordTest2() {
    MANAGER_WITH_KRYO_SER.setShuffleWriteMetrics(new ShuffleWriteMetrics());
    // after serialized: key + value = 31 byte, after compression, 33b
    String testKey = "testKey12345678901";
    String testValue = "testValue";
    List<ShuffleBlockInfo> result = MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
    assertEquals(0, result.size());
    assertEquals(64, MANAGER_WITH_KRYO_SER.getAllocatedBytes());
    assertEquals(1, MANAGER_WITH_KRYO_SER.getBuffers().size());
    result = MANAGER_WITH_KRYO_SER.addRecord(1, testKey, testValue);
    assertEquals(0, result.size());
    assertEquals(128, MANAGER_WITH_KRYO_SER.getAllocatedBytes());
    assertEquals(2, MANAGER_WITH_KRYO_SER.getBuffers().size());
    result = MANAGER_WITH_KRYO_SER.addRecord(2, testKey, testValue);
    assertEquals(3, result.size());
    assertEquals(0, MANAGER_WITH_KRYO_SER.getAllocatedBytes());
    assertEquals(0, MANAGER_WITH_KRYO_SER.getBuffers().size());
    result.stream().forEach(e -> assertEquals(33, e.getData().length));

    assertEquals(3, MANAGER_WITH_KRYO_SER.getShuffleWriteMetrics().recordsWritten());
    assertEquals(99, MANAGER_WITH_KRYO_SER.getShuffleWriteMetrics().bytesWritten());
  }

  @Test
  public void addRecordTest3() {
    // after serialized: key + value = 49 byte, after compression, 37b
    String testKey = "testKey12345678901testKey12345678901";
    String testValue = "testValue";
    // record > buffer size, flush
    List<ShuffleBlockInfo> result = MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
    assertEquals(1, result.size());
    assertEquals(0, MANAGER_WITH_KRYO_SER.getAllocatedBytes());
    assertEquals(0, MANAGER_WITH_KRYO_SER.getBuffers().size());
    result.stream().forEach(e -> assertEquals(37, e.getData().length));
  }

  @Test
  public void javaSerializeTest() {
    // after serialized: key + value = 20 byte, after compression, 38b
    String testKey = "testKey";
    String testValue = "testValue";
    List<ShuffleBlockInfo> result = MANAGER_WITH_JAVA_SER.addRecord(0, testKey, testValue);
    // single buffer is not full, there is no data return
    assertEquals(0, result.size());
    assertEquals(64, MANAGER_WITH_JAVA_SER.getAllocatedBytes());
    assertEquals(1, MANAGER_WITH_JAVA_SER.getBuffers().size());
    result = MANAGER_WITH_JAVA_SER.addRecord(0, testKey, testValue);
    // single buffer is full
    assertEquals(1, result.size());
    assertEquals(38, result.get(0).getData().length);
    assertEquals(0, MANAGER_WITH_JAVA_SER.getAllocatedBytes());
    assertEquals(0, MANAGER_WITH_JAVA_SER.getBuffers().size());
  }

  @Test
  public void clearTest() {
    // after serialized: key + value = 31 byte, after compression, 33b
    String testKey = "testKey12345678901";
    String testValue = "testValue";
    MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
    MANAGER_WITH_KRYO_SER.addRecord(1, testKey, testValue);
    // clear buffer and get all block
    List<ShuffleBlockInfo> result = MANAGER_WITH_KRYO_SER.clear();
    assertEquals(2, result.size());
    assertEquals(0, MANAGER_WITH_KRYO_SER.getAllocatedBytes());
    assertEquals(0, MANAGER_WITH_KRYO_SER.getBuffers().size());
    result.stream().forEach(e -> assertEquals(33, e.getData().length));
  }
}
