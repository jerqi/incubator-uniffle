package com.tencent.rss.server;

import com.tencent.rss.proto.RssProtos.StatusCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class ShuffleTaskManagerTest {

  private ShuffleTaskManager shuffleTaskManager;

  @Before
  public void setUp() {
    shuffleTaskManager = ShuffleTaskManager.mock();
  }

  @After
  public void tearDown() {
    shuffleTaskManager.clear();
  }

  @Test
  public void registerShuffleTest() throws IOException, IllegalStateException {
    ShuffleEngineManager mockEngineManager = mock(ShuffleEngineManager.class);
    when(mockEngineManager.registerShuffleEngine(1, 10))
      .thenReturn(StatusCode.SUCCESS);
    when(mockEngineManager.registerShuffleEngine(11, 20))
      .thenReturn(StatusCode.SUCCESS);
    when(mockEngineManager.registerShuffleEngine(21, 30))
      .thenReturn(StatusCode.SUCCESS);

    shuffleTaskManager.registerShuffle("test", "1", 1, 10, mockEngineManager);
    shuffleTaskManager.registerShuffle("test", "1", 11, 20, mockEngineManager);
    shuffleTaskManager.registerShuffle("test", "1", 21, 30, mockEngineManager);

    String key = ShuffleTaskManager.constructKey("test", "1");
    ShuffleEngineManager shuffleEngineManager = shuffleTaskManager
      .getShuffleTaskEngines()
      .get(key);

    assertEquals(mockEngineManager, shuffleEngineManager);
    assertEquals(1, shuffleTaskManager.getShuffleTaskEngines().size());

  }

  @Test
  public void registerShuffleConcurrentTest() throws IOException, InterruptedException, ExecutionException {
    ShuffleEngineManager mockEngineManager = mock(ShuffleEngineManager.class);
    when(mockEngineManager.registerShuffleEngine(1, 10))
      .thenReturn(StatusCode.SUCCESS);
    when(mockEngineManager.registerShuffleEngine(11, 20))
      .thenReturn(StatusCode.SUCCESS);
    when(mockEngineManager.registerShuffleEngine(21, 30))
      .thenReturn(StatusCode.SUCCESS);

    ExecutorService executorService = Executors.newFixedThreadPool(3);
    List<Callable<StatusCode>> calls = new ArrayList<>();

    for (int i = 1; i < 30; i += 10) {
      int start = i;
      int end = i + 9;
      calls.add(
        () -> shuffleTaskManager.registerShuffle("test", "1", start, end, mockEngineManager));
    }

    List<Future<StatusCode>> results = executorService.invokeAll(calls);
    for (Future<StatusCode> f : results) {
      assertEquals(StatusCode.SUCCESS, f.get());
    }

    String key = ShuffleTaskManager.constructKey("test", "1");
    ShuffleEngineManager shuffleEngineManager = shuffleTaskManager
      .getShuffleTaskEngines()
      .get(key);

    assertEquals(mockEngineManager, shuffleEngineManager);
    assertEquals(1, shuffleTaskManager.getShuffleTaskEngines().size());

  }

  @Test
  public void getShuffleEngineTest() throws IOException, IllegalStateException {
    ShuffleEngineManager mockEngineManager = mock(ShuffleEngineManager.class);
    when(mockEngineManager.registerShuffleEngine(1, 10))
      .thenReturn(StatusCode.SUCCESS);
    when(mockEngineManager.registerShuffleEngine(11, 20))
      .thenReturn(StatusCode.SUCCESS);
    when(mockEngineManager.registerShuffleEngine(21, 30))
      .thenReturn(StatusCode.SUCCESS);

    ShuffleEngine shuffleEngine1 = new ShuffleEngine("test", "1", 1, 10);
    ShuffleEngine shuffleEngine2 = new ShuffleEngine("test", "1", 11, 20);
    ShuffleEngine shuffleEngine3 = new ShuffleEngine("test", "1", 21, 30);
    when(mockEngineManager.getShuffleEngine(1)).thenReturn(shuffleEngine1);
    when(mockEngineManager.getShuffleEngine(13)).thenReturn(shuffleEngine2);
    when(mockEngineManager.getShuffleEngine(25)).thenReturn(shuffleEngine3);

    shuffleTaskManager.registerShuffle("test", "1", 1, 10, mockEngineManager);
    shuffleTaskManager.registerShuffle("test", "1", 11, 20, mockEngineManager);
    shuffleTaskManager.registerShuffle("test", "1", 21, 30, mockEngineManager);

    ShuffleEngine actual1 = shuffleTaskManager.getShuffleEngine("test", "1", 1);
    assertEquals(shuffleEngine1, actual1);

  }

  @Test
  public void commitShuffleTest() throws IOException, IllegalStateException {
    ShuffleEngineManager mockEngineManager = mock(ShuffleEngineManager.class);
    when(mockEngineManager.registerShuffleEngine(1, 10))
      .thenReturn(StatusCode.SUCCESS);
    when(mockEngineManager.registerShuffleEngine(11, 20))
      .thenReturn(StatusCode.SUCCESS);
    when(mockEngineManager.registerShuffleEngine(21, 30))
      .thenReturn(StatusCode.SUCCESS);

    when(mockEngineManager.commit()).thenReturn(StatusCode.SUCCESS);

    shuffleTaskManager.registerShuffle("test", "1", 1, 10, mockEngineManager);
    shuffleTaskManager.registerShuffle("test", "1", 11, 20, mockEngineManager);
    shuffleTaskManager.registerShuffle("test", "1", 21, 30, mockEngineManager);

    StatusCode actual = shuffleTaskManager.commitShuffle("test", "1");
    StatusCode expected = StatusCode.SUCCESS;
    assertEquals(expected, actual);

    actual = shuffleTaskManager.commitShuffle("test", "2");
    expected = StatusCode.NO_REGISTER;
    assertEquals(expected, actual);

  }
}
