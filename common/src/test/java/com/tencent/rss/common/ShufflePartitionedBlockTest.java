package com.tencent.rss.common;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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

    ShufflePartitionedBlock b1 = new ShufflePartitionedBlock(1, 1, 2, 3, 1, buf);
    assertEquals(1, b1.getLength());
    assertEquals(2, b1.getCrc());
    assertEquals(3, b1.getBlockId());

    ShufflePartitionedBlock b3 = new ShufflePartitionedBlock(1, 1, 2, 3, 3, buf);
    assertArrayEquals(buf, b3.getData());
  }
}
