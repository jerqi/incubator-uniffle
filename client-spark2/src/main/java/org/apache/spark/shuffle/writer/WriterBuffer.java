package org.apache.spark.shuffle.writer;

import com.google.common.collect.Lists;
import java.util.List;

public class WriterBuffer {

  private long copyTime = 0;
  private byte[] buffer;
  private int bufferSize;
  private int nextOffset = 0;
  private List<WrappedBuffer> buffers = Lists.newArrayList();
  private int dataLength = 0;

  public WriterBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
    this.buffer = new byte[bufferSize];
  }

  public void addRecord(byte[] recordBuffer, int length) {
    if (askForMemory(length)) {
      buffers.add(new WrappedBuffer(buffer, nextOffset));
      buffer = new byte[bufferSize];
      nextOffset = 0;
    }
    System.arraycopy(recordBuffer, 0, buffer, nextOffset, length);
    nextOffset += length;
    dataLength += length;
  }

  public boolean askForMemory(long length) {
    return nextOffset + length > bufferSize;
  }

  public byte[] getData() {
    byte[] data = new byte[dataLength];
    int offset = 0;
    long start = System.currentTimeMillis();
    for (WrappedBuffer wrappedBuffer : buffers) {
      System.arraycopy(wrappedBuffer.getBuffer(), 0, data, offset, wrappedBuffer.getSize());
      offset += wrappedBuffer.getSize();
    }
    // nextOffset is the length of current buffer used
    System.arraycopy(buffer, 0, data, offset, nextOffset);
    copyTime += System.currentTimeMillis() - start;
    return data;
  }

  public int getDataLength() {
    return dataLength;
  }

  public long getCopyTime() {
    return copyTime;
  }

  public int getMemoryUsed() {
    // list of buffers + current buffer
    return (buffers.size() + 1) * bufferSize;
  }

  private static final class WrappedBuffer {

    byte[] buffer;
    int size;

    WrappedBuffer(byte[] buffer, int size) {
      this.buffer = buffer;
      this.size = size;
    }

    public byte[] getBuffer() {
      return buffer;
    }

    public int getSize() {
      return size;
    }
  }
}
