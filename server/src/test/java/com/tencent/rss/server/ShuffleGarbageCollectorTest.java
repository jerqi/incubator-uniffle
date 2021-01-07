package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class ShuffleGarbageCollectorTest extends MetricsTestBase {

  private static final String confFile = ClassLoader.getSystemResource("gcTest.conf").getFile();
  private ShuffleServer shuffleServer;
  private ShuffleGarbageCollector shuffleGarbageCollector;

  @Before
  public void setUp() throws UnknownHostException, FileNotFoundException {
    shuffleServer = new ShuffleServer(confFile);
    shuffleGarbageCollector = new ShuffleGarbageCollector(shuffleServer);
    shuffleServer.getShuffleTaskManager().registerShuffle("testApp1", "s1", 0, 2);
    shuffleServer.getShuffleTaskManager().registerShuffle("testApp1", "s1", 3, 4);
    shuffleServer.getShuffleTaskManager().registerShuffle("testApp2", "s1", 0, 4);
    shuffleServer.getShuffleTaskManager().registerShuffle("testApp2", "s1", 5, 10);
    shuffleServer.getShuffleTaskManager().registerShuffle("testApp3", "s1", 0, 10);
  }

  @Test
  public void testGC() throws Exception {
    shuffleServer.getShuffleTaskManager().commitShuffle("testApp1", "s1");
    shuffleServer.getShuffleTaskManager().commitShuffle("testApp2", "s1");
    ShuffleEngine shuffleEngine = getShuffleEngine("testApp3", "s1", "0", "10");
    shuffleEngine.setTimestamp(Long.MAX_VALUE);
    Thread.sleep(5000);
    shuffleGarbageCollector.doGC();
    shuffleGarbageCollector.getExecutorService().shutdown();
    shuffleGarbageCollector.getExecutorService().awaitTermination(5, TimeUnit.SECONDS);
    assertEquals(1, shuffleServer.getShuffleTaskManager().getShuffleTaskEngines().size());
    assertEquals(1, getShuffleEngineManager("testApp3", "s1").getEngineMap().size());
    assertEquals(shuffleEngine, getShuffleEngineManager("testApp3", "s1").getEngineMap().get("0~10"));
  }

  private ShuffleEngineManager getShuffleEngineManager(String appId, String shuffleId) {
    String k = ShuffleTaskManager.constructKey(appId, shuffleId);
    return shuffleServer.getShuffleTaskManager().getShuffleTaskEngines().get(k);
  }

  private ShuffleEngine getShuffleEngine(String appId, String shuffleId, String start, String end) {
    String k1 = ShuffleTaskManager.constructKey(appId, shuffleId);
    String k2 = ShuffleTaskManager.constructKey(start, end);
    return shuffleServer
        .getShuffleTaskManager()
        .getShuffleTaskEngines()
        .get(k1)
        .getEngineMap()
        .get(k2);
  }
}
