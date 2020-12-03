package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tencent.rss.proto.RssProtos.StatusCode;
import java.io.IOException;
import org.junit.Test;

public class ShuffleEngineManagerTest {

    private ShuffleEngineManager shuffleEngineManager = new ShuffleEngineManager("test", "1");
    private ShuffleEngine mockShuffleEngine = mock(ShuffleEngine.class);

    @Test
    public void registerShuffleEngineTest() throws IOException, IllegalStateException {
        when(mockShuffleEngine.init()).thenReturn(StatusCode.SUCCESS);
        StatusCode actual = shuffleEngineManager.registerShuffleEngine(1, 10, mockShuffleEngine);
        StatusCode expected = StatusCode.SUCCESS;
        assertEquals(expected, actual);

    }

    @Test
    public void getShuffleEngineTest() throws IOException, IllegalStateException {
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
