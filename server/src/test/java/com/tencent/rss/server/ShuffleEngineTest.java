package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.storage.HdfsTestBase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShuffleEngineTest extends HdfsTestBase {

  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();
  private static final ShuffleServerConf conf = new ShuffleServerConf(confFile);
  private BufferManager bufferManager;

  @BeforeClass
  public static void staticSetUp() {
    ShuffleServerMetrics.register();
    conf.setString(ShuffleServerConf.DATA_STORAGE_BASE_PATH, HDFS_URI);
  }

  @Before
  public void setUp() {
    bufferManager = new BufferManager(1, 128, 0);
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
    StatusCode actual1 = writeData(shuffleEngine, len, 3);

    assertEquals(StatusCode.SUCCESS, actual1);
    ShuffleBuffer buffer = shuffleEngine.getBuffer();

    assertFalse(buffer.full());
    int expectedSize = 8 + 4 + 8 + len; // sum of (length, crc, blockId)'s type length + bytes
    assertEquals(expectedSize, buffer.getSize());

    // flush data and clear all data in the buffer
    len = 128;
    StatusCode actual2 = writeData(shuffleEngine, len, 3);
    assertEquals(StatusCode.SUCCESS, actual2);
    assertEquals(0, buffer.getSize());

    checkFiles(path, "test.index", "test.data");
  }

  @Test
  public void testWriteCommit() throws IOException {
    ShuffleEngine shuffleEngine =
        new ShuffleEngine("1", "2", 3, 4, conf, bufferManager, "test");

    assertEquals(1, shuffleEngine.getBufferManager().getAvailableCount());
    assertEquals(StatusCode.SUCCESS, shuffleEngine.init());
    assertEquals(0, shuffleEngine.getBufferManager().getAvailableCount());

    String basePath = shuffleEngine.getBasePath();
    Path path = new Path(basePath);

    // data is in the buffer
    StatusCode actual1 = writeData(shuffleEngine, 64, 3);
    ShuffleBuffer buffer = shuffleEngine.getBuffer();
    assertEquals(StatusCode.SUCCESS, actual1);
    assertFalse(buffer.full());

    shuffleEngine.commit();
    assertNull(shuffleEngine.getBuffer());
    assertTrue(shuffleEngine.getIsCommit());
    assertNull(shuffleEngine.getBuffer());

    StatusCode actual2 = writeData(shuffleEngine, 128, 4);
    assertEquals(StatusCode.SUCCESS, actual2);
    assertEquals(0, buffer.getSize());
    assertEquals(1, shuffleEngine.getBufferManager().getAtomicCount().get());
    assertEquals(0, shuffleEngine.getBufferManager().getAvailableCount());
    assertFalse(shuffleEngine.getIsCommit());
    assertNotNull(shuffleEngine.getBuffer());

    shuffleEngine.commit();
    assertEquals(0, shuffleEngine.getBufferManager().getAtomicCount().get());
    assertEquals(1, shuffleEngine.getBufferManager().getAvailableCount());
    assertTrue(shuffleEngine.getIsCommit());
    assertNull(shuffleEngine.getBuffer());
    shuffleEngine.commit();

    checkFiles(path, "test.index", "test.data");

    writeData(shuffleEngine, 32, 4);
    assertNotNull(shuffleEngine.getBuffer());
    assertFalse(shuffleEngine.getIsCommit());
    assertEquals(1, shuffleEngine.getBufferManager().getAtomicCount().get());
    assertEquals(0, shuffleEngine.getBufferManager().getAvailableCount());
    shuffleEngine.reclaim();
    assertNull(shuffleEngine.getBuffer());
    assertTrue(shuffleEngine.getIsCommit());
    assertEquals(0, shuffleEngine.getBufferManager().getAtomicCount().get());
    assertEquals(1, shuffleEngine.getBufferManager().getAvailableCount());

  }

  @Test
  public void testWriteCommitConcurrent() throws InterruptedException, ExecutionException, IOException {
    ShuffleEngine shuffleEngine =
        new ShuffleEngine("1", "2", 3, 4, conf, bufferManager, "test");
    String basePath = shuffleEngine.getBasePath();
    Path path = new Path(basePath);
    shuffleEngine.init();
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Callable<Void>> clients = new LinkedList<>();

    for (int i = 0; i < 10; ++i) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          boolean cur = new Random().nextBoolean();
          if (cur) {
            shuffleEngine.commit();
            assertNull(shuffleEngine.getBuffer());
            assertTrue(shuffleEngine.getIsCommit());
          } else {
            writeData(shuffleEngine, 64, 3);
            assertNotNull(shuffleEngine.getBuffer());
            assertFalse(shuffleEngine.getIsCommit());
          }
          return null;
        }
      };
      clients.add(callable);
    }

    List<Future<Void>> futures = executorService.invokeAll(clients);
    for (Future<Void> f : futures) {
      f.get();
    }
    checkFiles(path, "test.index", "test.data");
  }

  private StatusCode writeData(ShuffleEngine shuffleEngine, int len, int partition) throws IOException {
    byte[] d1 = new byte[len];
    new Random().nextBytes(d1);
    ShufflePartitionedBlock block1 = new ShufflePartitionedBlock(len, 1, 1, d1);
    ShufflePartitionedData data1 = new ShufflePartitionedData(partition, block1);
    return shuffleEngine.write(Collections.singletonList(data1));
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
