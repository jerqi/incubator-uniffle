package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BufferManagerTest {

  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();
  private BufferManager bufferManager;

  @Before
  public void setUp() throws UnknownHostException, FileNotFoundException {
    ShuffleServer shuffleServer = new ShuffleServer(confFile);
    bufferManager = shuffleServer.getBufferManager();
    bufferManager.setCapacity(1001);
    bufferManager.setBufferSize(1024);
  }

  @After
  public void tearDown() {
    bufferManager.getAtomicSize().set(0);
  }

  @Test
  public void getBufferTest() {
    ShuffleEngine engine1 = mock(ShuffleEngine.class);
    when(engine1.makeKey()).thenReturn("e1");
    ShuffleEngine engine2 = mock(ShuffleEngine.class);
    when(engine2.makeKey()).thenReturn("e2");

    ShuffleBuffer buffer1 = bufferManager.getBuffer(engine1);
    ShuffleBuffer buffer2 = bufferManager.getBuffer(engine2);

    assertNotNull(buffer1);
    assertNotNull(buffer2);
    assertEquals(2, bufferManager.getPool().size());
    assertTrue(bufferManager.getPool().containsKey("e1"));
    assertTrue(bufferManager.getPool().containsKey("e2"));

    bufferManager.updateSize(1024);
    ShuffleEngine engine3 = mock(ShuffleEngine.class);
    when(engine1.makeKey()).thenReturn("e3");
    ShuffleBuffer buffer3 = bufferManager.getBuffer(engine3);
    assertNull(buffer3);
  }

  @Test
  public void getBufferConcurrentTest() throws InterruptedException, ExecutionException {

    ExecutorService executorService = Executors.newFixedThreadPool(9);
    List<Callable<Long>> calls = new ArrayList<>();

    assertEquals(0, bufferManager.getAtomicSize().longValue());

    long expected = 0;
    for (int i = 1; i < 10; ++i) {
      if (i < 5) {
        expected -= i;
      } else {
        expected += i;
      }

      long delta = i;
      Callable<Long> callable = new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          if (delta < 5) {
            return bufferManager.updateSize(-delta);
          } else {
            return bufferManager.updateSize(delta);
          }
        }
      };
      calls.add(callable);
    }

    List<Long> buffers = new LinkedList<>();

    List<Future<Long>> results = executorService.invokeAll(calls);
    for (Future<Long> f : results) {
      f.get();
    }

    assertEquals(expected, bufferManager.getAtomicSize().longValue());

  }

  @Test
  public void testFlushAndReclaim() throws InterruptedException {
    ShuffleEngine engine1 = mock(ShuffleEngine.class);
    when(engine1.makeKey()).thenReturn("e1");
    ShuffleEngine engine2 = mock(ShuffleEngine.class);
    when(engine2.makeKey()).thenReturn("e2");

    ShuffleBuffer buffer1 = bufferManager.getBuffer(engine1);
    ShuffleBuffer buffer2 = bufferManager.getBuffer(engine2);

    buffer1.append(createData(500));
    buffer2.append(createData(500));

    assertEquals(1000, bufferManager.getAtomicSize().intValue());
    assertFalse(bufferManager.isFull());
    buffer1.append(createData(1));

    Thread.sleep(2000);
    assertEquals(0, bufferManager.getAtomicSize().intValue());
    assertEquals(2, bufferManager.getPool().size());

    bufferManager.reclaim(Arrays.asList("e1", "e2"));
    assertEquals(0, bufferManager.getPool().size());
  }

  @Test
  public void testConcurrentFlush() throws InterruptedException, ExecutionException {
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<ShuffleBuffer> buffers = new ArrayList<>();
    //List<ShuffleEngine> engines = new LinkedList<>();

    for (int i = 0; i < 10; ++i) {
      String key = "key" + i;
      ShuffleEngine engine = mock(ShuffleEngine.class);
      when(engine.makeKey()).thenReturn(key);
      ShuffleBuffer buffer = bufferManager.getBuffer(engine);
      buffers.add(buffer);
      buffer.append(createData(100));
    }

    assertEquals(1000, bufferManager.getAtomicSize().intValue());

    List<Callable<Void>> callables = new LinkedList<>();
    for (int i = 0; i < 10; ++i) {
      final int idx = i;
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          ShuffleBuffer buffer = buffers.get(idx);
          buffer.append(createData(1));
          return null;
        }
      };
      callables.add(callable);
    }

    List<Future<Void>> futures = executorService.invokeAll(callables);
    for (Future<Void> f : futures) {
      f.get();
    }

    Thread.sleep(2000);
    assertTrue(bufferManager.getAtomicSize().intValue() <= 999);
  }


  private ShufflePartitionedData createData(int len) {
    byte[] buf = new byte[len];
    new Random().nextBytes(buf);
    ShufflePartitionedBlock block = new ShufflePartitionedBlock(len, 1, 1, buf);
    ShufflePartitionedData data = new ShufflePartitionedData(1, block);
    return data;
  }
}
