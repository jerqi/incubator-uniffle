package com.tencent.rss.storage;

import java.io.Closeable;
import java.io.EOFException;
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

public class FileBasedShuffleReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedShuffleReader.class);
  private Path path;
  private Configuration hadoopConf;
  private FSDataInputStream fsDataInputStream;

  public FileBasedShuffleReader(Path path, Configuration hadoopConf) {
    this.path = path;
    this.hadoopConf = hadoopConf;
  }

  public void createStream() throws IOException, IllegalStateException {
    FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);

    if (!fileSystem.isFile(path)) {
      String msg = path + " don't exist or is not a file.";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }

    fsDataInputStream = fileSystem.open(path);
  }

  public byte[] readData(FileBasedShuffleSegment segment) throws IOException, IllegalStateException {
    try {
      fsDataInputStream.seek(segment.getOffset());
      int length = (int) segment.getLength();
      byte[] buf = new byte[length];
      fsDataInputStream.readFully(buf);
      return buf;
    } catch (EOFException e) {
      String msg = "No data from the index " + segment + " of " + path;
      throw new IllegalStateException(msg);
    }
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

  private FileBasedShuffleSegment readIndex() throws IOException, IllegalStateException {
    ByteBuffer buf = ByteBuffer.allocate(8);

    long offset = getLongFromStream(buf, true);
    if (offset == -1) { // EOF
      return null;
    }

    long length = getLongFromStream(buf);
    long crc = getLongFromStream(buf);
    long blockId = getLongFromStream(buf);

    return new FileBasedShuffleSegment(offset, length, crc, blockId);
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

  public long getOffset() throws IOException {
    return fsDataInputStream.getPos();
  }

  @Override
  public synchronized void close() throws IOException {
    fsDataInputStream.close();
  }

}
