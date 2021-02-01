package org.apache.spark.shuffle.writer;

import static org.junit.Assert.assertEquals;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.junit.Test;

public class WriteBufferTest {

  @Test
  public void test() {
    SparkConf conf = new SparkConf(false);
    Serializer kryoSerializer = new KryoSerializer(conf);
    WriterBuffer wb = new WriterBuffer(kryoSerializer.newInstance(), 16, 32);
    assertEquals(0, wb.getLength());
    String key = "key";
    String value = "value";
    int[] result = wb.addRecord(key, value);
    assertEquals(12, result[WriterBuffer.INDEX_RECORD_SIZE]);
    assertEquals(0, result[WriterBuffer.INDEX_EXTRA_MEMORY]);
    assertEquals(12, wb.getLength());
    result = wb.addRecord(key, value);
    assertEquals(12, result[WriterBuffer.INDEX_RECORD_SIZE]);
    assertEquals(24, result[WriterBuffer.INDEX_EXTRA_MEMORY]);
    assertEquals(24, wb.getLength());
    result = wb.addRecord(key, value);
    assertEquals(12, result[WriterBuffer.INDEX_RECORD_SIZE]);
    assertEquals(0, result[WriterBuffer.INDEX_EXTRA_MEMORY]);
    assertEquals(36, wb.getLength());
    result = wb.addRecord(key, value);
    assertEquals(12, result[WriterBuffer.INDEX_RECORD_SIZE]);
    assertEquals(24, result[WriterBuffer.INDEX_EXTRA_MEMORY]);
    assertEquals(48, wb.getLength());
    assertEquals(48, wb.getData().length);
  }

}
