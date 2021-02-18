package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.Random;
import java.util.Set;
import org.junit.Test;

public class ShuffleBufferTest {

  static {
    ShuffleServerMetrics.register();
  }

  @Test
  public void appendTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(100);
    shuffleBuffer.append(createData(10));
    assertEquals(10, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());

    shuffleBuffer.append(createData(90));
    assertEquals(100, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());

    shuffleBuffer.append(createData(1));
    assertEquals(101, shuffleBuffer.getSize());
    assertTrue(shuffleBuffer.isFull());
  }

  @Test
  public void toFlushEventTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(100);
    Set<Long> expectedEventIds = Sets.newHashSet();
    ShuffleDataFlushEvent event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1);
    assertNull(event);
    shuffleBuffer.append(createData(10));
    assertEquals(10, shuffleBuffer.getSize());
    event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1);
    expectedEventIds.add(event.getEventId());
    assertEquals(10, event.getSize());
    assertEquals(0, shuffleBuffer.getSize());
    assertEquals(0, shuffleBuffer.getBlocks().size());

    shuffleBuffer.append(createData(10));
    event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1);
    expectedEventIds.add(event.getEventId());
    Set<Long> eventIds = shuffleBuffer.getAndClearEventIds();
    assertTrue(expectedEventIds.containsAll(eventIds));
    assertTrue(eventIds.containsAll(expectedEventIds));

    expectedEventIds.clear();
    shuffleBuffer.append(createData(10));
    event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1);
    expectedEventIds.add(event.getEventId());
    shuffleBuffer.append(createData(10));
    event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1);
    expectedEventIds.add(event.getEventId());
    eventIds = shuffleBuffer.getAndClearEventIds();
    assertTrue(expectedEventIds.containsAll(eventIds));
    assertTrue(eventIds.containsAll(expectedEventIds));
  }

  private ShufflePartitionedData createData(int len) {
    byte[] buf = new byte[len];
    new Random().nextBytes(buf);
    ShufflePartitionedBlock block = new ShufflePartitionedBlock(len, 1, 1, buf);
    ShufflePartitionedData data = new ShufflePartitionedData(1, block);
    return data;
  }

}
