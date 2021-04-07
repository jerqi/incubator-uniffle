package org.apache.spark.shuffle.writer;

import static org.junit.Assert.assertEquals;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.junit.Test;
import scala.reflect.ClassTag$;

public class WriteBufferTest {

  @Test
  public void test() {
    SparkConf conf = new SparkConf(false);
    Serializer kryoSerializer = new KryoSerializer(conf);
    WrappedByteArrayOutputStream arrayOutputStream = new WrappedByteArrayOutputStream(32);
    SerializationStream serializeStream = kryoSerializer.newInstance().serializeStream(arrayOutputStream);
    WriterBuffer wb = new WriterBuffer(32);
    assertEquals(32, wb.getMemoryUsed());
    assertEquals(0, wb.getDataLength());
    String key = "key";
    String value = "value";

    arrayOutputStream.reset();
    serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
    serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
    serializeStream.flush();
    byte[] serializedData = arrayOutputStream.getBuf();
    int serializedDataLength = arrayOutputStream.size();

    // size of serialized kv is 12
    wb.addRecord(serializedData, serializedDataLength);
    assertEquals(32, wb.getMemoryUsed());
    assertEquals(12, wb.getDataLength());
    wb.addRecord(serializedData, serializedDataLength);
    assertEquals(32, wb.getMemoryUsed());
    // case: data size < output buffer size, when getData(), [] + buffer with 24b = 24b
    assertEquals(24, wb.getData().length);
    wb.addRecord(serializedData, serializedDataLength);
    // case: data size > output buffer size, when getData(), [1 buffer] + buffer with 12 = 36b
    assertEquals(36, wb.getData().length);
    assertEquals(64, wb.getMemoryUsed());
    wb.addRecord(serializedData, serializedDataLength);
    wb.addRecord(serializedData, serializedDataLength);
    // case: data size > output buffer size, when getData(), 2 buffer + output with 12b = 60b
    assertEquals(60, wb.getData().length);
    assertEquals(96, wb.getMemoryUsed());
  }
}
