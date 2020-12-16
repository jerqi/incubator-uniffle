package com.tencent.rss.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class BufferManagerTest {

  private BufferManager bufferManager = BufferManager.instance();

  @Before
  public void setUp() {
    bufferManager.init(3, 128, 0);
  }

  @After
  public void tearDown() {
    bufferManager.getAtomicCount().set(0);
  }

  @Test
  public void getBufferTest() {
    ShuffleBuffer shuffleBuffer1 = bufferManager.getBuffer(1, 100);
    ShuffleBuffer shuffleBuffer2 = bufferManager.getBuffer(101, 200);
    assertEquals(128, shuffleBuffer1.getCapacity());
    assertEquals(128, shuffleBuffer2.getCapacity());
  }

  @Test
  public void getBufferConcurrentTest() throws InterruptedException, ExecutionException {

    ExecutorService executorService = Executors.newFixedThreadPool(9);
    List<Callable<ShuffleBuffer>> calls = new ArrayList<>();

    assertEquals(0, bufferManager.getAtomicCount().intValue());

    for (int i = 1; i < 10; ++i) {
      int start = i;
      int end = i + 100;
      calls.add(() -> bufferManager.getBuffer(start, end));
    }

    List<ShuffleBuffer> buffers = new LinkedList<>();

    List<Future<ShuffleBuffer>> results = executorService.invokeAll(calls);
    for (Future<ShuffleBuffer> f : results) {
      ShuffleBuffer cur = f.get();
      if (cur != null) {
        buffers.add(cur);
      }
    }
    assertEquals(4, bufferManager.getAtomicCount().intValue());
    assertEquals(3, buffers.size());

  }

}
