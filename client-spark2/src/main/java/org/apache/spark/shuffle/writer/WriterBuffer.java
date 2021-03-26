package org.apache.spark.shuffle.writer;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.serializer.SerializationStream;
import scala.reflect.ClassTag$;

public class WriterBuffer {

  private long copyTime = 0;
  private long serializeTime = 0;
  private List<Object> keys = Lists.newLinkedList();
  private List<Object> values = Lists.newLinkedList();
  private int estimatedSize = 0;

  public WriterBuffer() {
  }

  public void addRecord(Object key, Object value, long size) {
    keys.add(key);
    values.add(value);
    estimatedSize += size;
  }

  public byte[] getData(SerializationStream serializeStream, Output output, int outputBufferSize) {
    List<byte[]> buffers = Lists.newArrayList();
    int length = 0;
    // through all kv and generate serialized data which will be added to list
    Iterator<Object> keyIter = keys.iterator();
    Iterator<Object> valueIter = values.iterator();
    while (keyIter.hasNext()) {
      Object key = keyIter.next();
      Object value = valueIter.next();
      final long start = System.currentTimeMillis();
      serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
      serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
      serializeStream.flush();
      serializeTime += System.currentTimeMillis() - start;
      if (output.position() > outputBufferSize) {
        length += output.position();
        long s = System.currentTimeMillis();
        buffers.add(output.toBytes());
        copyTime += System.currentTimeMillis() - s;
        output.clear();
      }
    }

    if (buffers.size() == 0) {
      byte[] result = output.toBytes();
      output.clear();
      return result;
    }

    // check serialized data length and create array
    length += output.position();
    byte[] data = new byte[length];
    int nextStart = 0;
    long s = System.currentTimeMillis();
    // copy all data into result array
    for (byte[] buf : buffers) {
      System.arraycopy(buf, 0, data, nextStart, buf.length);
      nextStart += buf.length;
    }
    if (output.position() > 0) {
      System.arraycopy(output.toBytes(), 0, data, nextStart, output.position());
    }
    copyTime += System.currentTimeMillis() - s;
    output.clear();
    return data;
  }

  public int getEstimatedSize() {
    return estimatedSize;
  }

  public long getCopyTime() {
    return copyTime;
  }

  public long getSerializeTime() {
    return serializeTime;
  }
}
