package org.apache.spark.shuffle.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.tencent.rss.proto.RssProtos;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class WriteBufferManagerTest {

    private static WriteBufferManager MANAGER_WITH_KRYO_SER;
    private static WriteBufferManager MANAGER_WITH_JAVA_SER;

    @BeforeAll
    public static void init() {
        Serializer kryoSerializer = new KryoSerializer(new SparkConf(false));
        MANAGER_WITH_KRYO_SER = new WriteBufferManager(32, 64, 128, 0, kryoSerializer);
        Serializer javaSerializer = new KryoSerializer(new SparkConf(false));
        MANAGER_WITH_JAVA_SER = new WriteBufferManager(32, 64, 128, 0, javaSerializer);
    }

    @Test
    public void addRecordTest1() {
        // after serialized: key + value = 20 byte
        String testKey = "testKey";
        String testValue = "testValue";
        Map<Integer, RssProtos.ShuffleBlock> result = MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
        // single buffer is not full, there is no data return
        assertEquals(0, result.size());
        assertEquals(20, MANAGER_WITH_KRYO_SER.getTotalBytes());
        assertEquals(1, MANAGER_WITH_KRYO_SER.getBuffers().size());
        result = MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
        // single buffer is full, and the size
        assertEquals(1, result.size());
        assertEquals(40, result.get(0).getData().size());
        assertEquals(0, MANAGER_WITH_KRYO_SER.getTotalBytes());
        assertEquals(0, MANAGER_WITH_KRYO_SER.getBuffers().size());
    }

    @Test
    public void addRecordTest2() {
        // after serialized: key + value = 31 byte
        String testKey = "testKey12345678901";
        String testValue = "testValue";
        Map<Integer, RssProtos.ShuffleBlock> result = MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
        assertEquals(0, result.size());
        assertEquals(31, MANAGER_WITH_KRYO_SER.getTotalBytes());
        assertEquals(1, MANAGER_WITH_KRYO_SER.getBuffers().size());
        result = MANAGER_WITH_KRYO_SER.addRecord(1, testKey, testValue);
        assertEquals(0, result.size());
        assertEquals(62, MANAGER_WITH_KRYO_SER.getTotalBytes());
        assertEquals(2, MANAGER_WITH_KRYO_SER.getBuffers().size());
        result = MANAGER_WITH_KRYO_SER.addRecord(2, testKey, testValue);
        assertEquals(0, result.size());
        assertEquals(93, MANAGER_WITH_KRYO_SER.getTotalBytes());
        assertEquals(3, MANAGER_WITH_KRYO_SER.getBuffers().size());
        result = MANAGER_WITH_KRYO_SER.addRecord(3, testKey, testValue);
        assertEquals(0, result.size());
        assertEquals(124, MANAGER_WITH_KRYO_SER.getTotalBytes());
        assertEquals(4, MANAGER_WITH_KRYO_SER.getBuffers().size());
        // the total buffer is larger than 128, spill happen
        result = MANAGER_WITH_KRYO_SER.addRecord(4, testKey, testValue);
        assertEquals(5, result.size());
        assertEquals(0, MANAGER_WITH_KRYO_SER.getTotalBytes());
        assertEquals(0, MANAGER_WITH_KRYO_SER.getBuffers().size());
        result.entrySet().stream().forEach(e -> assertEquals(31, e.getValue().getData().size()));
    }

    @Test
    public void javaSerializeTest() {
        // after serialized: key + value = 20 byte
        String testKey = "testKey";
        String testValue = "testValue";
        Map<Integer, RssProtos.ShuffleBlock> result = MANAGER_WITH_JAVA_SER.addRecord(0, testKey, testValue);
        // single buffer is not full, there is no data return
        assertEquals(0, result.size());
        assertEquals(20, MANAGER_WITH_JAVA_SER.getTotalBytes());
        assertEquals(1, MANAGER_WITH_JAVA_SER.getBuffers().size());
        result = MANAGER_WITH_JAVA_SER.addRecord(0, testKey, testValue);
        // single buffer is full, and the size
        assertEquals(1, result.size());
        assertEquals(40, result.get(0).getData().size());
        assertEquals(0, MANAGER_WITH_JAVA_SER.getTotalBytes());
        assertEquals(0, MANAGER_WITH_JAVA_SER.getBuffers().size());
    }

    @Test
    public void clearTest() {
        // after serialized: key + value = 31 byte
        String testKey = "testKey12345678901";
        String testValue = "testValue";
        MANAGER_WITH_KRYO_SER.addRecord(0, testKey, testValue);
        MANAGER_WITH_KRYO_SER.addRecord(1, testKey, testValue);
        MANAGER_WITH_KRYO_SER.addRecord(2, testKey, testValue);
        MANAGER_WITH_KRYO_SER.addRecord(3, testKey, testValue);
        // clear buffer and get all block
        Map<Integer, RssProtos.ShuffleBlock> result = MANAGER_WITH_KRYO_SER.clear();
        assertEquals(4, result.size());
        assertEquals(0, MANAGER_WITH_KRYO_SER.getTotalBytes());
        assertEquals(0, MANAGER_WITH_KRYO_SER.getBuffers().size());
        result.entrySet().stream().forEach(e -> assertEquals(31, e.getValue().getData().size()));
    }
}
