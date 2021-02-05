package org.apache.spark.shuffle.writer;

import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag$;

public class WriterBuffer {

  public static final int INDEX_RECORD_SIZE = 0;
  public static final int INDEX_EXTRA_MEMORY = 1;
  private SerializationStream serializeStream;
  private Output output;
  private byte[] buffer;
  private int serializerBufferSize;
  private int serializerMaxBufferSize;
  private long copyTime;

  public WriterBuffer(SerializerInstance instance, int serializerBufferSize, int serializerMaxBufferSize) {
    this.buffer = new byte[0];
    this.serializerBufferSize = serializerBufferSize;
    this.serializerMaxBufferSize = serializerMaxBufferSize;
    output = new Output(serializerBufferSize, serializerMaxBufferSize);
    serializeStream = instance.serializeStream(output);
    copyTime = 0;
  }

  // write record and check extra memory
  public int[] addRecord(Object key, Object value) {
    int oldPosition = output.position();
    serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
    serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
    serializeStream.flush();
    int recordSize = output.position() - oldPosition;
    int extraMemory = 0;
    if (output.position() > serializerBufferSize) {
      extraMemory = output.position();
      long s = System.currentTimeMillis();
      buffer = ArrayUtils.addAll(buffer, output.toBytes());
      copyTime += System.currentTimeMillis() - s;
      output.clear();
    }
    return new int[]{recordSize, extraMemory};
  }

  public byte[] getData() {
    if (buffer.length == 0) {
      return output.toBytes();
    } else if (output.position() == 0) {
      return buffer;
    }
    long s = System.currentTimeMillis();
    byte[] result = ArrayUtils.addAll(buffer, output.toBytes());
    copyTime += System.currentTimeMillis() - s;
    return result;
  }

  public int getLength() {
    return buffer.length + output.position();
  }

  public int getMemorySize() {
    if (buffer.length == 0) {
      return serializerMaxBufferSize;
    } else {
      return buffer.length + serializerMaxBufferSize;
    }
  }

  public void clear() {
    buffer = null;
    serializeStream.close();
  }

  public long getCopyTime() {
    return copyTime;
  }
}
