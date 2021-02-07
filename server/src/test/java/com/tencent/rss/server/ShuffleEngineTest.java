package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.storage.HdfsTestBase;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
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
  private ShuffleServer shuffleServer;
  private BufferManager bufferManager;

  @BeforeClass
  public static void staticSetUp() {
    ShuffleServerMetrics.register();
    conf.setString(ShuffleServerConf.DATA_STORAGE_BASE_PATH, HDFS_URI);
  }

  @Before
  public void setUp() throws UnknownHostException, FileNotFoundException {
    shuffleServer = new ShuffleServer(conf);
    bufferManager = shuffleServer.getBufferManager();
    bufferManager.setBufferSize(100);
  }

  @Test
  public void testBasePath() {
    ShuffleEngine shuffleEngine =
        new ShuffleEngine("1", 2, 3, 4,
            conf, bufferManager);
    shuffleEngine.init();
    String actual = shuffleEngine.getBasePath();
    String expected = HdfsTestBase.HDFS_URI + "/1/2/3-4";
    assertEquals(expected, actual);
  }

  @Test
  public void testWrite() throws Exception {
    ShuffleEngine shuffleEngine =
        new ShuffleEngine("1", 2, 3, 4,
            conf, bufferManager);
    shuffleEngine.init();
    String basePath = shuffleEngine.getBasePath();
    Path path = new Path(basePath);

    // data is in the buffer
    int len = 64;
    StatusCode actual1 = writeData(shuffleEngine, len, 3);

    assertEquals(StatusCode.SUCCESS, actual1);
    ShuffleBuffer buffer = shuffleEngine.getBuffer();

    assertFalse(buffer.isFull());
    assertEquals(len, buffer.getSize());
    assertEquals(len, bufferManager.getAtomicSize().intValue());

    // flush data and clear all data in the buffer
    len = 128;
    writeData(shuffleEngine, len, 3);
    assertEquals(0, buffer.getSize());
    checkFiles(path, shuffleServer.getId() + ".data", shuffleServer.getId() + ".index");
    assertEquals(0, bufferManager.getAtomicSize().intValue());
  }

  @Test
  public void testWriteCommit() throws IOException, InterruptedException {
    ShuffleEngine shuffleEngine =
        new ShuffleEngine("1", 2, 3, 4,
            conf, bufferManager);
    ShuffleEngine shuffleEngine1 =
        new ShuffleEngine("1", 2, 0, 2,
            conf, bufferManager);

    shuffleEngine.init();
    shuffleEngine1.init();
    assertEquals(shuffleEngine.getBufferManager(), bufferManager);
    assertEquals(shuffleEngine1.getBufferManager(), bufferManager);

    String basePath = shuffleEngine.getBasePath();
    Path path = new Path(basePath);

    // data is in the buffer
    writeData(shuffleEngine, 64, 3);
    writeData(shuffleEngine1, 6, 3);
    checkEngine(shuffleEngine, 64, 1, 70);
    checkEngine(shuffleEngine1, 6, 1, 70);

    shuffleEngine.commit();
    Thread.sleep(1000);
    checkEngine(shuffleEngine, 0, 0, 6);

    // buffer full and flush
    writeData(shuffleEngine, 128, 4);
    Thread.sleep(1000);
    checkEngine(shuffleEngine, 0, 0, 6);

    shuffleEngine1.commit();
    Thread.sleep(1000);
    checkEngine(shuffleEngine1, 0, 0, 0);

    checkFiles(path, shuffleServer.getId() + ".index", shuffleServer.getId() + ".data");

    // write after commit
    shuffleEngine.commit();
    writeData(shuffleEngine, 32, 4);
    checkEngine(shuffleEngine, 32, 1, 32);

  }

  @Test
  public void testWriteCommitConcurrent() throws InterruptedException, ExecutionException, IOException {
    ShuffleEngine shuffleEngine =
        new ShuffleEngine("1", 2, 3, 4,
            conf, bufferManager);
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
          } else {
            writeData(shuffleEngine, 64, 3);
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

    checkFiles(path, shuffleServer.getId() + ".data", shuffleServer.getId() + ".index");
  }

  private StatusCode writeData(ShuffleEngine shuffleEngine, int len, int partition) throws IOException {
    byte[] d1 = new byte[len];
    new Random().nextBytes(d1);
    ShufflePartitionedBlock block1 = new ShufflePartitionedBlock(len, 1, 1, d1);
    ShufflePartitionedData data1 = new ShufflePartitionedData(partition, block1);
    return shuffleEngine.write(data1);
  }

  private void checkEngine(ShuffleEngine shuffleEngine, int bufferSize, int blocksSize, int bufferManagerSize) {
    assertNotNull(shuffleEngine.getBuffer());
    assertEquals(bufferSize, shuffleEngine.getBuffer().getSize());
    assertEquals(blocksSize, shuffleEngine.getBuffer().getBlocks().size());
    assertEquals(bufferManagerSize, shuffleEngine.getBufferManager().getAtomicSize().intValue());
  }

  private void checkFiles(Path path, String... expected) throws IOException, InterruptedException {
    // wait for asyn flush
    Thread.sleep(2000);
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
