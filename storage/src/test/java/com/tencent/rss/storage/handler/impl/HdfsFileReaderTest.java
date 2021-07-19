package com.tencent.rss.storage.handler.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class HdfsFileReaderTest extends HdfsTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createStreamTest() throws IOException {
    Path path = new Path(HDFS_URI, "createStreamTest");
    fs.create(path);

    try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
      assertTrue(fs.isFile(path));
      assertEquals(0L, reader.getOffset());
    }

    fs.deleteOnExit(path);
  }

  @Test
  public void createStreamAppendTest() throws IOException {
    Path path = new Path(HDFS_URI, "createStreamFirstTest");

    assertFalse(fs.isFile(path));
    try {
      new HdfsFileReader(path, conf);
      fail("Exception should be thrown");
    } catch (IllegalStateException ise) {
      ise.getMessage().startsWith(HDFS_URI + "createStreamFirstTest don't exist");
    }
  }

  @Test
  public void readDataTest() throws IOException {
    Path path = new Path(HDFS_URI, "readDataTest");
    byte[] data = new byte[160];
    int offset = 128;
    int length = 32;
    new Random().nextBytes(data);
    long crc11 = ChecksumUtils.getCrc32(ByteBuffer.wrap(data, offset, length));

    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      writer.writeData(data);
    }
    FileBasedShuffleSegment segment = new FileBasedShuffleSegment(23, offset, length, length, 0xdeadbeef, 1);
    try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
      byte[] actual = reader.readData(segment.getOffset(), segment.getLength());
      long crc22 = ChecksumUtils.getCrc32(actual);

      for (int i = 0; i < length; ++i) {
        assertEquals(data[i + offset], actual[i]);
      }
      assertEquals(crc11, crc22);
      // EOF exception is expected
      segment = new FileBasedShuffleSegment(23, offset * 2, length, length, 1, 1);
      assertNull(reader.readData(segment.getOffset(), segment.getLength()));
    }
  }

  @Test
  public void readIndexTest() throws IOException {
    Path path = new Path(HDFS_URI, "readIndexTest");
    FileBasedShuffleSegment[] segments = {
        new FileBasedShuffleSegment(123, 0, 32, 32, 1, 1),
        new FileBasedShuffleSegment(223, 32, 23, 23, 2, 1),
        new FileBasedShuffleSegment(323, 64, 32, 32, 3, 2)
    };

    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      for (int i = 0; i < segments.length; ++i) {
        writer.writeIndex(segments[i]);
      }
    }

    try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
      // test limit
      int limit = 2;
      List<FileBasedShuffleSegment> idx = reader.readIndex(limit);
      assertEquals(2, idx.size());

      for (int i = 0; i < limit; ++i) {
        assertEquals(segments[i], idx.get(i));
      }

      long expected = 2 * (4 * 8 + 2 * 4); // segment length = 4 * 8 + 2 * 4
      assertEquals(expected, reader.getOffset());

      idx = reader.readIndex(1000);
      assertEquals(1, idx.size());
      assertEquals(segments[2], idx.get(0));

      expected = 3 * (4 * 8 + 2 * 4);
      assertEquals(expected, reader.getOffset());
    }
  }

  @Test
  public void readIndexFailTest() throws IOException {
    Path path = new Path(HDFS_URI, "readIndexFailTest");
    FileBasedShuffleSegment[] segments = {
        new FileBasedShuffleSegment(123, 0, 32, 32, 1, 1),
        new FileBasedShuffleSegment(223, 32, 23, 32, 2, 1),
        new FileBasedShuffleSegment(323, 64, 32, 32, 3, 2)
    };

    try (HdfsFileWriter writer = new HdfsFileWriter(path, conf)) {
      for (int i = 0; i < segments.length; ++i) {
        writer.writeIndex(segments[i]);
      }

      int[] data = {1, 3, 5, 7, 9};

      ByteBuffer buf = ByteBuffer.allocate(4 * data.length);
      buf.asIntBuffer().put(data);
      writer.writeData(buf.array());
    }

    try (HdfsFileReader reader = new HdfsFileReader(path, conf)) {
      // test limit
      int limit = 10;
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Invalid index file "
          + path + " " + 4 + " bytes left, can't be parsed as long.");
      reader.readIndex(limit);
    }
  }
}
