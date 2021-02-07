package com.tencent.rss.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.storage.common.BufferSegment;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.impl.HdfsShuffleReadHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleWriteHandler;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
    new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);
    Path path = new Path(basePath);
    assertTrue(fs.isDirectory(path));
  }

  @Test
  public void writeTest() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "writeTest";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 1, 0, 1, basePath, "test", conf);
    List<ShufflePartitionedBlock> blocks = new LinkedList<>();
    List<byte[]> expectedData = new LinkedList<>();
    List<FileBasedShuffleSegment> expectedIndex = new LinkedList<>();

    int pos = 0;
    for (int i = 1; i < 13; ++i) {
      byte[] buf = new byte[i * 8];
      new Random().nextBytes(buf);
      expectedData.add(buf);
      blocks.add(new ShufflePartitionedBlock(i * 8, i, i, buf));
      expectedIndex.add(new FileBasedShuffleSegment(pos, i * 8, i, i));
      pos += i * 8;
    }
    writeHandler.write(blocks);

    // a data file and a index is created after writing
    fs.isFile(new Path(basePath, "test.data"));
    fs.isFile(new Path(basePath, "test.index"));

    compareDataAndIndex("appId", 1, 1, basePath, expectedData);

    // append the exist data and index files
    List<ShufflePartitionedBlock> blocksAppend = new LinkedList<>();
    for (int i = 13; i < 23; ++i) {
      byte[] buf = new byte[i * 8];
      new Random().nextBytes(buf);
      expectedData.add(buf);
      blocksAppend.add(new ShufflePartitionedBlock(i * 8, i, i, buf));
      expectedIndex.add(new FileBasedShuffleSegment(pos, i * 8, i, i));
      pos += i * 8;
    }
    writeHandler.write(blocksAppend);

    compareDataAndIndex("appId", 1, 1, basePath, expectedData);
  }

  private void compareDataAndIndex(
      String appId,
      int shuffleId,
      int partitionId,
      String basePath,
      List<byte[]> expectedData) throws IOException, IllegalStateException {
    // read directly and compare
    HdfsShuffleReadHandler readHandler = new HdfsShuffleReadHandler(
        appId, shuffleId, partitionId, 100, 2, 10,
        10000, basePath, Sets.newHashSet(1L));
    try {
      List<byte[]> actual = readData(readHandler);
      compareBytes(expectedData, actual);
    } finally {
      readHandler.close();
    }
  }

  private List<byte[]> readData(HdfsShuffleReadHandler handler) throws IOException, IllegalStateException {
    Set<Long> blockIds = handler.getAllBlockIds();
    byte[] readBuffer = handler.readShuffleData(blockIds);
    Map<Long, BufferSegment> processingBlockIds = handler.getBlockIdToBufferSegment();
    List<byte[]> result = Lists.newArrayList();
    for (Map.Entry<Long, BufferSegment> entry : processingBlockIds.entrySet()) {
      BufferSegment bs = entry.getValue();
      byte[] data = new byte[bs.getLength()];
      System.arraycopy(readBuffer, bs.getOffset(), data, 0, bs.getLength());
      result.add(data);
    }
    return result;
  }

  private void compareBytes(List<byte[]> expected, List<byte[]> actual) {
    assertEquals(expected.size(), actual.size());

    for (int i = 0; i < expected.size(); i++) {
      assertArrayEquals(expected.get(i), actual.get(i));
    }
  }

}

