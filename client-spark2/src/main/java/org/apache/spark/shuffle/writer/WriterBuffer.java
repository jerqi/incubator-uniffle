package org.apache.spark.shuffle.writer;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag$;

public class WriterBuffer {

  public static final int INDEX_RECORD_SIZE = 0;
  public static final int INDEX_EXTRA_MEMORY = 1;
  private SerializationStream serializeStream;
  private Output output;
  private List<byte[]> buffers = Lists.newArrayList();
  private int serializerBufferSize;
  private int memorySize;
  private int dataLength = 0;
  private long copyTime = 0;
  private long serializeTime = 0;

  public WriterBuffer(SerializerInstance instance, int serializerBufferSize, int serializerMaxBufferSize) {
    this.serializerBufferSize = serializerBufferSize;
    this.memorySize = serializerMaxBufferSize;
    output = new Output(serializerBufferSize, serializerMaxBufferSize);
    serializeStream = instance.serializeStream(output);
  }

  // write record and check extra memory
  public int[] addRecord(Object key, Object value) {
    final int oldPosition = output.position();
    final long start = System.currentTimeMillis();
    serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
    serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
    serializeStream.flush();
    serializeTime += System.currentTimeMillis() - start;
    int recordSize = output.position() - oldPosition;
    int extraMemory = 0;
    if (output.position() > serializerBufferSize) {
      extraMemory = output.position();
      long s = System.currentTimeMillis();
      buffers.add(output.toBytes());
      copyTime += System.currentTimeMillis() - s;
      memorySize += extraMemory;
      dataLength += extraMemory;
      output.clear();
    }
    return new int[]{recordSize, extraMemory};
  }

  public byte[] getData() {
    if (buffers.size() == 0) {
      return output.toBytes();
    }
    byte[] data = new byte[getLength()];
    int nextStart = 0;
    long s = System.currentTimeMillis();
    for (byte[] buf : buffers) {
      System.arraycopy(buf, 0, data, nextStart, buf.length);
      nextStart += buf.length;
    }
    if (output.position() > 0) {
      System.arraycopy(output.toBytes(), 0, data, nextStart, output.position());
    }
    copyTime += System.currentTimeMillis() - s;
    return data;
  }

  public int getLength() {
    return dataLength + output.position();
  }

  public int getMemorySize() {
    return memorySize;
  }

  public void clear() {
    buffers.clear();
    serializeStream.close();
  }

  public long getCopyTime() {
    return copyTime;
  }

  public long getSerializeTime() {
    return serializeTime;
  }
}
