package org.apache.spark.shuffle.writer;

import java.io.ByteArrayOutputStream;

/**
 * Subclass of ByteArrayOutputStream that exposes `buf` directly.
 */
public class WrappedByteArrayOutputStream extends ByteArrayOutputStream {

  public WrappedByteArrayOutputStream(int size) {
    super(size);
  }

  public byte[] getBuf() {
    return buf;
  }
}
