package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileWriter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileWriter.class);

  private DataOutputStream dataOutputStream;
  private long initSize;
  private long nextOffset;

  public LocalFileWriter(File file) throws IOException {
    // init fsDataOutputStream
    dataOutputStream = new DataOutputStream(new FileOutputStream(file, true));
    initSize = file.length();
    nextOffset = initSize;
  }


  public void writeData(ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer.hasArray()) {
      dataOutputStream.write(
          byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining());
    } else {
      byte[] byteArray = new byte[byteBuffer.remaining()];
      byteBuffer.get(byteArray);
      dataOutputStream.write(byteArray);
    }
    nextOffset = initSize + dataOutputStream.size();
  }

  public void writeIndex(FileBasedShuffleSegment segment) throws IOException {
    dataOutputStream.writeLong(segment.getOffset());
    dataOutputStream.writeInt(segment.getLength());
    dataOutputStream.writeInt(segment.getUncompressLength());
    dataOutputStream.writeLong(segment.getCrc());
    dataOutputStream.writeLong(segment.getBlockId());
  }

  public long nextOffset() {
    return nextOffset;
  }

  @Override
  public synchronized void close() throws IOException {
    if (dataOutputStream != null) {
      dataOutputStream.close();
    }
  }

}
