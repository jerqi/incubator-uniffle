package com.tencent.rss.storage.handler.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.protobuf.ByteString;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HdfsFileWriterTest extends HdfsTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createStreamFirstTest() throws IOException {
    Path path = new Path(HDFS_URI, "createStreamFirstTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertTrue(fs.isFile(path));
      assertEquals(0, writer.nextOffset());
    }
  }

  @Test
  public void createStreamAppendTest() throws IOException {
    byte[] data = new byte[32];
    new Random().nextBytes(data);
    ByteString byteString = ByteString.copyFrom(data);

    // create a file and fill 32 bytes
    Path path = new Path(HDFS_URI, "createStreamAppendTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertEquals(0, writer.nextOffset());
      writer.writeData(byteString.asReadOnlyByteBuffer());
      assertEquals(32, writer.nextOffset());
    }

    // open existing file using append
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertTrue(fs.isFile(path));
      assertEquals(32, writer.nextOffset());
    }

    // disable the append support
    conf.setBoolean("dfs.support.append", false);
    assertTrue(fs.isFile(path));
    try {
      new HdfsFileWriter(path, conf);
      fail("Exception should be thrown");
    } catch (IllegalStateException ise) {
      assertTrue(ise.getMessage().startsWith(path + " exists but append mode is not support!"));
    }
  }

  @Test
  public void createStreamDirectory() throws IOException {
    // create a file and fill 32 bytes
    Path path = new Path(HDFS_URI, "createStreamDirectory");
    fs.mkdirs(path);

    try {
      new HdfsFileWriter(path, conf);
      fail("Exception should be thrown");
    } catch (IllegalStateException ise) {
      assertTrue(ise.getMessage().startsWith(HDFS_URI + "createStreamDirectory is a directory!"));
    }
  }

  @Test
  public void createStreamTest() throws IOException {
    byte[] data = new byte[32];
    new Random().nextBytes(data);
    ByteBuffer buf = ByteBuffer.allocate(32);
    buf.put(data);
    Path path = new Path(HDFS_URI, "createStreamTest");

    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertEquals(0, writer.nextOffset());
      buf.flip();
      writer.writeData(buf);
      assertEquals(32, writer.nextOffset());
    }
  }

  @Test(expected = EOFException.class)
  public void writeBufferTest() throws IOException {
    byte[] data = new byte[32];
    new Random().nextBytes(data);
    ByteString byteString = ByteString.copyFrom(data);

    Path path = new Path(HDFS_URI, "writeBufferTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertEquals(0, writer.nextOffset());
      writer.writeData(byteString.asReadOnlyByteBuffer());
      assertEquals(32, writer.nextOffset());
    }

    FileSystem fs = path.getFileSystem(conf);
    try (FSDataInputStream in = fs.open(path)) {
      for (int i = 0; i < data.length; ++i) {
        assertEquals(data[i], in.readByte());
      }
      // EOF exception is expected
      in.readInt();
    }

  }

  @Test(expected = EOFException.class)
  public void writeBufferArrayTest() throws IOException {
    int[] data = {1, 3, 5, 7, 9};

    ByteBuffer buf = ByteBuffer.allocate(4 * data.length);
    buf.asIntBuffer().put(data);

    Path path = new Path(HDFS_URI, "writeBufferArrayTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      assertEquals(0, writer.nextOffset());
      writer.writeData(buf);
      assertEquals(20, writer.nextOffset());
    }

    FileSystem fs = path.getFileSystem(conf);
    try (FSDataInputStream in = fs.open(path)) {
      for (int i = 0; i < data.length; ++i) {
        assertEquals(data[i], in.readInt());
      }
      // EOF exception is expected
      in.readInt();
    }
  }

  @Test
  public void writeSegmentTest() throws IOException {
    FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
        23, 128, 32, 32, 0xdeadbeef);

    Path path = new Path(HDFS_URI, "writeSegmentTest");
    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      writer.writeIndex(segment);
    }

    FileSystem fs = path.getFileSystem(conf);
    try (FSDataInputStream in = fs.open(path)) {
      assertEquals(128, in.readLong());
      assertEquals(32, in.readInt());
      assertEquals(32, in.readInt());
      assertEquals(0xdeadbeef, in.readLong());
      assertEquals(23, in.readLong());
    }
  }
}
