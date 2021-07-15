package com.tencent.rss.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ShufflePartitionedBlockTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void shufflePartitionedBlockTest() {
    byte[] buf = new byte[3];
    new Random().nextBytes(buf);

    ShufflePartitionedBlock b1 = new ShufflePartitionedBlock(1, 1, 2, 3, 1, ByteBuffer.wrap(buf));
    assertEquals(1, b1.getLength());
    assertEquals(2, b1.getCrc());
    assertEquals(3, b1.getBlockId());

    ByteBuffer bb = ByteBuffer.wrap(buf);
    ShufflePartitionedBlock b2 = new ShufflePartitionedBlock(1, 1, 2, 3, 1, bb);
    assertEquals(bb, b2.getData());
    assertArrayEquals(buf, b2.getData().array());

    ShufflePartitionedBlock b3 = new ShufflePartitionedBlock(1, 1, 2, 3, 3, ByteBuffer.wrap(buf));
    assertArrayEquals(buf, b3.getData().array());
  }
}
