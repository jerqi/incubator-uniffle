package com.tencent.rss.server;

import com.tencent.rss.proto.RssProtos.StatusCode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShuffleEngineManagerTest extends MetricsTestBase {

  private ShuffleEngineManager shuffleEngineManager = new ShuffleEngineManager("test", "1", null);
  private ShuffleEngine mockShuffleEngine = mock(ShuffleEngine.class);

  @Test
  public void registerShuffleEngineTest() {
    when(mockShuffleEngine.init()).thenReturn(StatusCode.SUCCESS);
    StatusCode actual = shuffleEngineManager.registerShuffleEngine(1, 10, mockShuffleEngine);
    StatusCode expected = StatusCode.SUCCESS;
    assertEquals(expected, actual);

  }

  @Test
  public void getShuffleEngineTest() {
    when(mockShuffleEngine.init()).thenReturn(StatusCode.SUCCESS);
    shuffleEngineManager.registerShuffleEngine(1, 10, mockShuffleEngine);
    ShuffleEngine actual1 = shuffleEngineManager.getShuffleEngine(1);
    assertEquals(mockShuffleEngine, actual1);
    actual1 = shuffleEngineManager.getShuffleEngine(10);
    assertEquals(mockShuffleEngine, actual1);
    ShuffleEngine actual2 = shuffleEngineManager.getShuffleEngine(11);
    assertEquals(null, actual2);

  }

}
