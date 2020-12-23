package com.tencent.rss.server;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.storage.HdfsTestBase;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ShuffleEngineTest extends HdfsTestBase {
  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();
  private static final ShuffleServerConf conf = new ShuffleServerConf(confFile);
  private static final BufferManager bufferManager = new BufferManager(1, 128, 0);

  @BeforeClass
  public static void setUp() {
    ShuffleServerMetrics.register();
    conf.loadConfFromFile(confFile);
    conf.setString(ShuffleServerConf.DATA_STORAGE_BASE_PATH, HDFS_URI);
  }

  @Test
  public void testBasePath() {
    ShuffleEngine shuffleEngine =
      new ShuffleEngine("1", "2", 3, 4, conf, bufferManager, "test");
    shuffleEngine.init();
    String actual = shuffleEngine.getBasePath();
    String expected = HdfsTestBase.HDFS_URI + "/1_2_3-4";
    assertEquals(expected, actual);
  }

  @Test
  public void testWrite() throws IOException {
    ShuffleEngine shuffleEngine =
      new ShuffleEngine("1", "2", 3, 4, conf, bufferManager, "test");
    shuffleEngine.init();
    String basePath = shuffleEngine.getBasePath();
    Path path = new Path(basePath);

    // data is in the buffer
    int len = 64;
    byte[] d1 = new byte[len];
    new Random().nextBytes(d1);
    ShufflePartitionedBlock block1 = new ShufflePartitionedBlock(len, 1, 1, d1);
    ShufflePartitionedData data1 = new ShufflePartitionedData(3, block1);
    StatusCode actual1 = shuffleEngine.write(Collections.singletonList(data1));
    assertEquals(StatusCode.SUCCESS, actual1);
    ShuffleBuffer buffer = shuffleEngine.getBuffer();

    assertFalse(buffer.full());
    int expectedSize = 8 + 4 + 8 + 64; // sum of (length, crc, blockId)'s type length + bytes
    assertEquals(expectedSize, buffer.getSize());

    // flush data and clear all data in the buffer
    len = 128;
    byte[] d2 = new byte[len];
    new Random().nextBytes(d2);
    ShufflePartitionedBlock block2 = new ShufflePartitionedBlock(len, 1, 2, d2);
    ShufflePartitionedData data2 = new ShufflePartitionedData(3, block2);
    StatusCode actual2 = shuffleEngine.write(Collections.singletonList(data2));

    assertEquals(StatusCode.SUCCESS, actual2);
    assertEquals(0, buffer.getSize());
    checkFiles(path, "test.index", "test.data");
  }

  private void checkFiles(Path path, String... expected) throws IOException {
    List<String> files = new ArrayList<>();
    for (FileStatus status : fs.listStatus(path)) {
      files.add(status.getPath().getName());
    }
    assertEquals(2, files.size());
    Collections.sort(files);
    Arrays.sort(expected);
    assertEquals(Arrays.asList(expected), files);
  }

}
