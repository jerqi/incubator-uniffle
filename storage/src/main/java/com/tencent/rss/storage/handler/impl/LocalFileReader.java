package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.api.ShuffleReader;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileReader implements ShuffleReader, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileReader.class);
  private String path;
  private DataInputStream dataInputStream;

  public LocalFileReader(String path) throws Exception {
    this.path = path;
    dataInputStream = new DataInputStream(new FileInputStream(path));
  }

  public byte[] readData(long offset, int length) {
    try {
      dataInputStream.skip(offset);
      byte[] buf = new byte[length];
      dataInputStream.readFully(buf);
      return buf;
    } catch (Exception e) {
      LOG.warn("Can't read data for path:" + path + " with offset[" + offset + "], length[" + length + "]", e);
    }
    return null;
  }

  public List<FileBasedShuffleSegment> readIndex(int limit) throws IOException, IllegalStateException {
    List<FileBasedShuffleSegment> ret = new LinkedList<>();

    for (int i = 0; i < limit; ++i) {
      FileBasedShuffleSegment segment = readIndex();
      if (segment == null) {
        break;
      }
      ret.add(segment);
    }

    return ret;
  }

  public FileBasedShuffleSegment readIndex() throws IOException, IllegalStateException {
    if (dataInputStream.available() <= 0) {
      return null;
    }

    long offset = dataInputStream.readLong();
    int length = dataInputStream.readInt();
    int uncompressLength = dataInputStream.readInt();
    long crc = dataInputStream.readLong();
    long blockId = dataInputStream.readLong();
    long taskAttemptId = dataInputStream.readLong();
    return new FileBasedShuffleSegment(blockId, offset, length, uncompressLength, crc, taskAttemptId);
  }

  public void skip(long offset) throws IOException {
    dataInputStream.skip(offset);
  }

  @Override
  public synchronized void close() {
    if (dataInputStream != null) {
      try {
        dataInputStream.close();
      } catch (IOException ioe) {
        LOG.warn("Error happen when close " + path, ioe);
      }
    }
  }
}
