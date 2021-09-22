package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;

import com.tencent.rss.common.PartitionRange;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BasicAssignmentStrategyTest {

  private SimpleClusterManager clusterManager;
  private BasicAssignmentStrategy strategy;

  @Before
  public void setUp() {
    CoordinatorConf ssc = new CoordinatorConf();
    clusterManager = new SimpleClusterManager(ssc);
    strategy = new BasicAssignmentStrategy(clusterManager);
  }

  @After
  public void tearDown() {
    clusterManager.clear();
  }

  @Test
  public void testNextId() {
    assertEquals(1, strategy.nextIdx(0, 3));
    assertEquals(2, strategy.nextIdx(1, 3));
    assertEquals(0, strategy.nextIdx(2, 3));
  }

  @Test
  public void testGenerateRanges() {
    List<PartitionRange> ranges = strategy.generateRanges(16, 5);
    assertEquals(new PartitionRange(0, 4), ranges.get(0));
    assertEquals(new PartitionRange(5, 9), ranges.get(1));
    assertEquals(new PartitionRange(10, 14), ranges.get(2));
    assertEquals(new PartitionRange(15, 19), ranges.get(3));
  }

  @Test
  public void testAssign() {
    for (int i = 0; i < 20; ++i) {
      clusterManager.add(new ServerNode(String.valueOf(i), "", 0, 0, 0, 20 - i, 0));
    }

    PartitionRangeAssignment pra = strategy.assign(100, 10, 2);
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    assertEquals(10, assignments.size());

    for (int i = 0; i < 100; i += 10) {
      assignments.containsKey(new PartitionRange(i, i + 10));
    }

    int i = 0;
    Iterator<List<ServerNode>> ite = assignments.values().iterator();
    while (ite.hasNext()) {
      List<ServerNode> cur = ite.next();
      assertEquals(2, cur.size());
      assertEquals(String.valueOf(i++), cur.get(0).getId());
      assertEquals(String.valueOf(i++), cur.get(1).getId());
    }
  }
}
