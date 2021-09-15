package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.Random;
import org.junit.Test;

public class ShuffleBufferTest {

  static {
    ShuffleServerMetrics.register();
  }

  @Test
  public void appendTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(100);
    shuffleBuffer.append(createData(10));
    assertEquals(42, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());

    shuffleBuffer.append(createData(26));
    assertEquals(100, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());

    shuffleBuffer.append(createData(1));
    assertEquals(133, shuffleBuffer.getSize());
    assertTrue(shuffleBuffer.isFull());
  }

  @Test
  public void toFlushEventTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(100);
    ShuffleDataFlushEvent event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, null);
    assertNull(event);
    shuffleBuffer.append(createData(10));
    assertEquals(42, shuffleBuffer.getSize());
    event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, null);
    assertEquals(42, event.getSize());
    assertEquals(0, shuffleBuffer.getSize());
    assertEquals(0, shuffleBuffer.getBlocks().size());
  }

  private ShufflePartitionedData createData(int len) {
    byte[] buf = new byte[len];
    new Random().nextBytes(buf);
    ShufflePartitionedBlock block = new ShufflePartitionedBlock(len, len, 1, 1, 1, buf);
    ShufflePartitionedData data = new ShufflePartitionedData(1, new ShufflePartitionedBlock[]{block});
    return data;
  }

}
