package com.tencent.rss.server;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.booleanThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShuffleBufferTest extends MetricsTestBase {

  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();
  private BufferManager bufferManager;
  private ShuffleBuffer shuffleBuffer;

  @Before
  public void setUp() throws UnknownHostException, FileNotFoundException {
    ShuffleServer shuffleServer = new ShuffleServer(confFile);
    bufferManager = shuffleServer.getBufferManager();
    bufferManager.setCapacity(200);
    bufferManager.setBufferSize(100);

    ShuffleEngine engine = mock(ShuffleEngine.class);
    when(engine.makeKey()).thenReturn("key");
    shuffleBuffer = bufferManager.getBuffer(engine);
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testAppend() {
    shuffleBuffer.append(createData(10));
    assertEquals(10, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());
  }

  @Test
  public void testFlush() {
    shuffleBuffer.append(createData(10));
    ShuffleDataFlushEvent event = shuffleBuffer.flush();
    assertEquals(10, event.getSize());
    assertEquals(0, shuffleBuffer.getSize());
    assertNull(shuffleBuffer.flush());
    assertEquals(0, shuffleBuffer.getBlocks().size());
    assertEquals(0, shuffleBuffer.getSize());
  }

  private ShufflePartitionedData createData(int len) {
    byte[] buf = new byte[len];
    new Random().nextBytes(buf);
    ShufflePartitionedBlock block = new ShufflePartitionedBlock(len, 1, 1, buf);
    ShufflePartitionedData data = new ShufflePartitionedData(1, block);
    return data;
  }

}
