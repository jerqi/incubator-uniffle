package org.apache.spark.shuffle.writer;

import static org.junit.Assert.assertEquals;

import com.esotericsoftware.kryo.io.Output;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.util.SizeEstimator$;
import org.junit.Test;

public class WriteBufferTest {

  @Test
  public void test() {
    SparkConf conf = new SparkConf(false);
    Serializer kryoSerializer = new KryoSerializer(conf);
    testWithSerializer(kryoSerializer);
    Serializer javaSerializer = new JavaSerializer(conf);
    testWithSerializer(javaSerializer);
  }

  public void testWithSerializer(Serializer serializer) {
    Output output = new Output(32, 64);
    SerializationStream serializeStream = serializer.newInstance().serializeStream(output);
    WriterBuffer wb = new WriterBuffer();
    assertEquals(0, wb.getEstimatedSize());
    String key = "key";
    String value = "value";
    long estimatedSize = SizeEstimator$.MODULE$.estimate(key) + SizeEstimator$.MODULE$.estimate(value);
    wb.addRecord(key, value, estimatedSize);
    assertEquals(estimatedSize, wb.getEstimatedSize());
    wb.addRecord(key, value, estimatedSize);
    assertEquals(estimatedSize * 2, wb.getEstimatedSize());
    if (serializer instanceof KryoSerializer) {
      validateKryoSerializer(wb, serializeStream, output, key, value, estimatedSize);
    } else {
      validateJavaSerializer(wb, serializeStream, output, key, value, estimatedSize);
    }
  }

  private void validateKryoSerializer(
      WriterBuffer wb, SerializationStream serializeStream,
      Output output, String key, String value, long estimatedSize) {
    // size of serialized kv is 12
    // case: data size < output buffer size, when getData(), 0 buffer + output with 24b
    assertEquals(24, wb.getData(serializeStream, output, 32).length);
    wb.addRecord(key, value, estimatedSize);
    // case: data size > output buffer size, when getData(), 1 buffer + output with 0b
    assertEquals(36, wb.getData(serializeStream, output, 32).length);
    wb.addRecord(key, value, estimatedSize);
    wb.addRecord(key, value, estimatedSize);
    wb.addRecord(key, value, estimatedSize);
    wb.addRecord(key, value, estimatedSize);
    // case: data size > output buffer size, when getData(), 2 buffer + output with 12b
    assertEquals(84, wb.getData(serializeStream, output, 32).length);
    wb.addRecord(key, value, estimatedSize);
    wb.addRecord(key, value, estimatedSize);
    // case: data size > output buffer size, when getData(), 3 buffer + output with 0b
    assertEquals(108, wb.getData(serializeStream, output, 32).length);
  }

  private void validateJavaSerializer(
      WriterBuffer wb, SerializationStream serializeStream,
      Output output, String key, String value, long estimatedSize) {
    assertEquals(28, wb.getData(serializeStream, output, 32).length);
    wb.addRecord(key, value, estimatedSize);
    assertEquals(30, wb.getData(serializeStream, output, 32).length);
  }
}
