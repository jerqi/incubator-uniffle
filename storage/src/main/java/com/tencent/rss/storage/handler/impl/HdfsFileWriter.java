package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileWriter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileWriter.class);

  private Path path;
  private Configuration hadoopConf;
  private FSDataOutputStream fsDataOutputStream;
  private long nextOffset;

  public HdfsFileWriter(Path path, Configuration hadoopConf) throws IOException, IllegalStateException {
    // init fsDataOutputStream
    this.path = path;
    this.hadoopConf = hadoopConf;
    initStream();
  }

  private void initStream() throws IOException, IllegalStateException {
    FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);
    if (fileSystem.isFile(path)) {
      if (hadoopConf.getBoolean("dfs.support.append", true)) {
        fsDataOutputStream = fileSystem.append(path);
        nextOffset = fsDataOutputStream.getPos();
      } else {
        String msg = path + " exists but append mode is not support!";
        LOG.error(msg);
        throw new IllegalStateException(msg);
      }
    } else if (fileSystem.isDirectory(path)) {
      String msg = path + " is a directory!";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    } else {
      fsDataOutputStream = fileSystem.create(path);
      nextOffset = fsDataOutputStream.getPos();
    }
  }

  public void writeData(ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer.hasArray()) {
      fsDataOutputStream.write(
          byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining());
    } else {
      byte[] byteArray = new byte[byteBuffer.remaining()];
      byteBuffer.get(byteArray);
      fsDataOutputStream.write(byteArray);
    }
    nextOffset = fsDataOutputStream.getPos();
  }

  public void writeIndex(FileBasedShuffleSegment segment) throws IOException {
    fsDataOutputStream.writeLong(segment.getOffset());
    fsDataOutputStream.writeInt(segment.getLength());
    fsDataOutputStream.writeInt(segment.getUncompressLength());
    fsDataOutputStream.writeLong(segment.getCrc());
    fsDataOutputStream.writeLong(segment.getBlockId());
    fsDataOutputStream.writeLong(segment.getTaskAttemptId());
  }

  public long nextOffset() {
    return nextOffset;
  }

  public void flush() throws IOException {
    if (fsDataOutputStream != null) {
      fsDataOutputStream.flush();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (fsDataOutputStream != null) {
      fsDataOutputStream.close();
    }
  }

//  private void flush() throws IOException {
//    try {
//      fsDataOutputStream.hflush();
//      // Useful for local file system where hflush/sync does not work (HADOOP-7844)
//      fsDataOutputStream.getWrappedStream().flush();
//    } catch (IOException e) {
//      logger.error("Fail to flush output stream of {}, {}", path, e.getCause());
//      throw e;
//    }
//  }

}
