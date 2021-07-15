package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.tencent.rss.common.PartitionRange;
import org.junit.Test;

public class PartitionRangeTest {

  @Test
  public void test() {
    PartitionRange range1 = new PartitionRange(0, 5);
    PartitionRange range2 = new PartitionRange(0, 5);
    assertFalse(range1 == range2);
    assertEquals(range1, range2);
    assertEquals(0, range1.getStart());
    assertEquals(5, range1.getEnd());
  }

}
