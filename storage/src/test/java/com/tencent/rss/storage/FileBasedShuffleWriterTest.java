package com.tencent.rss.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileBasedShuffleWriterTest extends HdfsTestBase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void createStreamFirstTest() throws IOException {
        Path path = new Path(HDFS_URI, "createStreamFirstTest");
        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            assertFalse(fs.exists(path));
            writer.createStream();
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
        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            writer.createStream();
            assertEquals(0, writer.nextOffset());
            writer.writeData(byteString.asReadOnlyByteBuffer());
            assertEquals(32, writer.nextOffset());
        }

        // open existing file using append
        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            assertTrue(fs.isFile(path));
            writer.createStream();
            assertEquals(32, writer.nextOffset());
        }

        // disable the append support
        conf.setBoolean("dfs.support.append", false);
        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            assertTrue(fs.isFile(path));
            thrown.expect(IllegalStateException.class);
            thrown.expectMessage(path + " exists but append mode is not support!");
            writer.createStream();
        }
    }

    @Test
    public void createStreamDirectory() throws IOException {
        // create a file and fill 32 bytes
        Path path = new Path(HDFS_URI, "createStreamDirectory");
        fs.mkdirs(path);

        // open existing file using append
        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            assertTrue(fs.isDirectory(path));
            thrown.expect(IllegalStateException.class);
            thrown.expectMessage(HDFS_URI + "createStreamDirectory is a directory!");
            writer.createStream();
        }

    }


    @Test
    public void createStreamTest() throws IOException {
        byte[] data = new byte[32];
        new Random().nextBytes(data);
        ByteBuffer buf = ByteBuffer.allocate(32);
        buf.put(data);
        Path path = new Path(HDFS_URI, "createStreamTest");

        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            writer.createStream();
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
        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            writer.createStream();
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
        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            assertEquals(0, writer.nextOffset());
            writer.createStream();
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
        FileBasedShuffleSegment segment = new FileBasedShuffleSegment(128, 32, 0xdeadbeef, 23);

        Path path = new Path(HDFS_URI, "writeSegmentTest");
        try (FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, conf)) {
            writer.createStream();
            writer.writeIndex(segment);
        }

        FileSystem fs = path.getFileSystem(conf);
        try (FSDataInputStream in = fs.open(path)) {
            assertEquals(128, in.readLong());
            assertEquals(32, in.readLong());
            assertEquals(0xdeadbeef, in.readLong());
            assertEquals(23, in.readLong());
        }

    }
}
