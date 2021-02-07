package com.tencent.rss.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.impl.FileBasedShuffleReader;
import com.tencent.rss.storage.handler.impl.FileBasedShuffleWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.fs.Path;
import org.hamcrest.core.StringStartsWith;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileBasedShuffleReaderTest extends HdfsTestBase {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createStreamTest() throws IOException {
    Path path = new Path(HDFS_URI, "createStreamTest");
    fs.create(path);

    try (FileBasedShuffleReader reader = new FileBasedShuffleReader(path, conf)) {
      reader.createStream();
      assertTrue(fs.isFile(path));
      assertEquals(0L, reader.getOffset());
    }

    fs.deleteOnExit(path);
  }

  @Test
  public void createStreamAppendTest() throws IOException {
    Path path = new Path(HDFS_URI, "createStreamFirstTest");

    assertFalse(fs.isFile(path));
    try (FileBasedShuffleReader reader = new FileBasedShuffleReader(path, conf)) {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage(StringStartsWith.startsWith(HDFS_URI + "createStreamFirstTest don't exist"));
      reader.createStream();
    }
  }

  @Test
  public void readDataTest() throws IOException {
    Path path = new Path(HDFS_URI, "readDataTest");
    byte[] data = new byte[160];
    new Random().nextBytes(data);

    try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
      ByteString byteString = ByteString.copyFrom(data);
      writer.createStream();
      writer.writeData(byteString.asReadOnlyByteBuffer());
    }

    int offset = 128;
    int length = 32;
    FileBasedShuffleSegment segment = new FileBasedShuffleSegment(offset, length, 0xdeadbeef, 23);
    try (FileBasedShuffleReader reader = new FileBasedShuffleReader(path, conf)) {
      reader.createStream();
      byte[] actual = reader.readData(segment);
      for (int i = 0; i < length; ++i) {
        assertEquals(data[i + offset], actual[i]);
      }

      // EOF exception is expected
      segment = new FileBasedShuffleSegment(offset * 2, length, 1, 23);
      assertNull(reader.readData(segment));
    }
  }

  @Test
  public void readIndexTest() throws IOException {
    Path path = new Path(HDFS_URI, "readIndexTest");
    FileBasedShuffleSegment[] segments = {
        new FileBasedShuffleSegment(0, 32, 1, 123),
        new FileBasedShuffleSegment(32, 23, 2, 223),
        new FileBasedShuffleSegment(64, 32, 3, 323)
    };

    try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
      writer.createStream();
      for (int i = 0; i < segments.length; ++i) {
        writer.writeIndex(segments[i]);
      }
    }

    try (FileBasedShuffleReader reader = new FileBasedShuffleReader(path, conf)) {
      reader.createStream();

      // test limit
      int limit = 2;
      List<FileBasedShuffleSegment> idx = reader.readIndex(limit);
      assertEquals(2, idx.size());

      for (int i = 0; i < limit; ++i) {
        assertEquals(segments[i], idx.get(i));
      }

      long expected = 2 * (4 * 8); // segment length = 4 * 8
      assertEquals(expected, reader.getOffset());

      idx = reader.readIndex(1000);
      assertEquals(1, idx.size());
      assertEquals(segments[2], idx.get(0));

      expected = 3 * (4 * 8);
      assertEquals(expected, reader.getOffset());
    }
  }

  @Test
  public void readIndexFailTest() throws IOException {
    Path path = new Path(HDFS_URI, "readIndexFailTest");
    FileBasedShuffleSegment[] segments = {
        new FileBasedShuffleSegment(0, 32, 1, 123),
        new FileBasedShuffleSegment(32, 23, 2, 223),
        new FileBasedShuffleSegment(64, 32, 3, 323)
    };

    try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
      writer.createStream();
      for (int i = 0; i < segments.length; ++i) {
        writer.writeIndex(segments[i]);
      }

      int[] data = {1, 3, 5, 7, 9};

      ByteBuffer buf = ByteBuffer.allocate(4 * data.length);
      buf.asIntBuffer().put(data);
      writer.writeData(buf);
    }

    try (FileBasedShuffleReader reader = new FileBasedShuffleReader(path, conf)) {
      reader.createStream();

      // test limit
      int limit = 10;
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("Invalid index file "
          + path + " " + 4 + " bytes left, can't be parsed as long.");
      List<FileBasedShuffleSegment> idx = reader.readIndex(limit);
    }
  }

}
