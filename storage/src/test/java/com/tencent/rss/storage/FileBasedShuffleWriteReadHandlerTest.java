package com.tencent.rss.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileBasedShuffleWriteReadHandlerTest extends HdfsTestBase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void initTest() throws IOException {
        String basePath = HDFS_URI + "test_base";
        new FileBasedShuffleWriteHandler(basePath, conf);
        Path path = new Path(basePath);
        assertTrue(fs.isDirectory(path));
    }

    @Test
    public void writeTest() throws IOException, IllegalStateException {
        String basePath = HDFS_URI + "writeTest";
        FileBasedShuffleWriteHandler writeHandler = new FileBasedShuffleWriteHandler(basePath, conf);

        List<ByteString> data = new ArrayList<>();
        List<ShuffleBlock> blocks = new LinkedList<>();
        List<FileBasedShuffleSegment> expectedIndex = new LinkedList<>();

        int pos = 0;
        for (int i = 1; i < 13; ++i) {
            byte[] buf = new byte[i * 8];
            new Random().nextBytes(buf);
            ByteString bs = ByteString.copyFrom(buf);
            data.add(bs);
            blocks.add(ShuffleBlock.newBuilder().setBlockId(i).setCrc(i).setData(bs).build());
            expectedIndex.add(new FileBasedShuffleSegment(pos, i * 8, i, i));
            pos += i * 8;
        }
        writeHandler.write(blocks);

        // a data file and a index is created after writing
        fs.isFile(new Path(basePath, "data"));
        fs.isFile(new Path(basePath, "index"));

        compareDataAndIndex(basePath, blocks, expectedIndex);

        // append the exist data and index files
        List<ShuffleBlock> blocksAppend = new LinkedList<>();
        for (int i = 13; i < 23; ++i) {
            byte[] buf = new byte[i * 8];
            new Random().nextBytes(buf);
            ByteString bs = ByteString.copyFrom(buf);
            data.add(bs);
            blocksAppend.add(ShuffleBlock.newBuilder().setBlockId(i).setCrc(i).setData(bs).build());
            expectedIndex.add(new FileBasedShuffleSegment(pos, i * 8, i, i));
            pos += i * 8;
        }
        writeHandler.write(blocksAppend);
        blocks.addAll(blocksAppend);

        compareDataAndIndex(basePath, blocks, expectedIndex);
    }

    private void compareDataAndIndex(
            String path,
            List<ShuffleBlock> blocks,
            List<FileBasedShuffleSegment> index) throws IOException, IllegalStateException {
        // read directly and compare
        try (FileBasedShuffleReadHandler readHandler = new FileBasedShuffleReadHandler(path, conf)) {
            List<ShuffleBlock> actual = readHandler.readData();
            assertEquals(blocks, actual);
        }

        // read index and use the index to read data
        try (FileBasedShuffleReadHandler readHandler = new FileBasedShuffleReadHandler(path, conf)) {
            assertEquals(1024 * 1024, readHandler.getIndexReadLimit());
            assertEquals(1024, readHandler.getDataReadLimit());
            List<FileBasedShuffleSegment> actualIndex = readHandler.readIndex();
            assertEquals(index, actualIndex);

            List<ShuffleBlock> actual = readHandler.readData(actualIndex);
            assertEquals(blocks, actual);
        }
    }
}

