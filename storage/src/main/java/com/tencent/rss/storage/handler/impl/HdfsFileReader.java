package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.api.ShuffleReader;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileReader implements ShuffleReader, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileReader.class);
  private Path path;
  private Configuration hadoopConf;
  private FSDataInputStream fsDataInputStream;

  public HdfsFileReader(Path path, Configuration hadoopConf) throws IOException, IllegalStateException {
    this.path = path;
    this.hadoopConf = hadoopConf;
    createStream();
  }

  private void createStream() throws IOException, IllegalStateException {
    FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);

    if (!fileSystem.isFile(path)) {
      String msg = path + " don't exist or is not a file.";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    fsDataInputStream = fileSystem.open(path);
  }

  public byte[] readData(long offset, int length) {
    try {
      fsDataInputStream.seek(offset);
      byte[] buf = new byte[length];
      fsDataInputStream.readFully(buf);
      return buf;
    } catch (Exception e) {
      LOG.warn("Can't read data for path:" + path + " with offset["
          + offset + "], length[" + length + "]", e);
    }
    return null;
  }

  public void seek(long offset) throws Exception {
    fsDataInputStream.seek(offset);
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
    ByteBuffer longBuf = ByteBuffer.allocate(8);
    ByteBuffer intBuf = ByteBuffer.allocate(4);

    long offset = getLongFromStream(longBuf, true);
    if (offset == -1) { // EOF
      return null;
    }

    int length = getIntegerFromStream(intBuf);
    int uncompressLength = getIntegerFromStream(intBuf);
    long crc = getLongFromStream(longBuf);
    long blockId = getLongFromStream(longBuf);
    long taskAttemptId = getLongFromStream(longBuf);

    return new FileBasedShuffleSegment(blockId, offset, length, uncompressLength, crc, taskAttemptId);
  }

  private long getLongFromStream(ByteBuffer buf) throws IOException, IllegalStateException {
    return getLongFromStream(buf, false);
  }

  /**
   * The segment in the index file must be strictly aligned, either reach EOF when read the first
   * param (offset) of the segment or succeed to readLong.
   */
  private long getLongFromStream(ByteBuffer buf, boolean isFirst) throws IOException, IllegalStateException {
    int len = fsDataInputStream.read(buf);
    if (isFirst && (len == 0 || len == -1)) { // EOF
      return -1;
    }

    long ret = 0L;
    try {
      buf.flip();
      ret = buf.getLong();
      buf.clear();
    } catch (BufferUnderflowException e) {
      String msg = "Invalid index file " + path + " " + len + " bytes left, can't be parsed as long.";
      throw new IllegalStateException(msg);
    }

    return ret;
  }

  private int getIntegerFromStream(ByteBuffer buf) throws IOException, IllegalStateException {
    int len = fsDataInputStream.read(buf);
    if (len == -1) { // EOF
      return -1;
    }

    int ret = 0;
    try {
      buf.flip();
      ret = buf.getInt();
      buf.clear();
    } catch (BufferUnderflowException e) {
      String msg = "Invalid index file " + path + " " + len + " bytes left, can't be parsed as int.";
      throw new IllegalStateException(msg);
    }

    return ret;
  }

  public long getOffset() throws IOException {
    return fsDataInputStream.getPos();
  }

  @Override
  public synchronized void close() throws IOException {
    if (fsDataInputStream != null) {
      fsDataInputStream.close();
    }
  }

}
