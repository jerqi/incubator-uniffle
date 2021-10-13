package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import com.tencent.rss.common.PartitionRange;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BasicAssignmentStrategyTest {

  Set<String> tags = Sets.newHashSet("test");
  private SimpleClusterManager clusterManager;
  private BasicAssignmentStrategy strategy;
  private int shuffleNodesMax = 7;

  @Before
  public void setUp() {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, shuffleNodesMax);
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
      clusterManager.add(new ServerNode(String.valueOf(i), "", 0, 0, 0,
          20 - i, 0, tags));
    }

    PartitionRangeAssignment pra = strategy.assign(100, 10, 2, tags);
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
      assertEquals(String.valueOf(i % shuffleNodesMax), cur.get(0).getId());
      i++;
      assertEquals(String.valueOf(i % shuffleNodesMax), cur.get(1).getId());
      i++;
    }
  }

  @Test
  public void testRandomAssign() {
    for (int i = 0; i < 20; ++i) {
      clusterManager.add(new ServerNode(String.valueOf(i), "", 0, 0, 0,
          0, 0, tags));
    }
    PartitionRangeAssignment pra = strategy.assign(100, 10, 2, tags);
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    Set<ServerNode> serverNodes1 = Sets.newHashSet();
    for (Map.Entry<PartitionRange, List<ServerNode>> assignment : assignments.entrySet()) {
      serverNodes1.addAll(assignment.getValue());
    }

    pra = strategy.assign(100, 10, 2, tags);
    assignments = pra.getAssignments();
    Set<ServerNode> serverNodes2 = Sets.newHashSet();
    for (Map.Entry<PartitionRange, List<ServerNode>> assignment : assignments.entrySet()) {
      serverNodes2.addAll(assignment.getValue());
    }

    // test for the random node pick, there is a little possibility failed
    assertFalse(serverNodes1.containsAll(serverNodes2));
  }

  @Test
  public void testAssignWithDifferentNodeNum() {
    ServerNode sn1 = new ServerNode("sn1", "", 0, 0, 0,
        20, 0, tags);
    ServerNode sn2 = new ServerNode("sn2", "", 0, 0, 0,
        10, 0, tags);
    ServerNode sn3 = new ServerNode("sn3", "", 0, 0, 0,
        0, 0, tags);

    clusterManager.add(sn1);
    PartitionRangeAssignment pra = strategy.assign(100, 10, 2, tags);
    // nodeNum < replica
    assertNull(pra.getAssignments());

    // nodeNum = replica
    clusterManager.add(sn2);
    pra = strategy.assign(100, 10, 2, tags);
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    Set<ServerNode> serverNodes = Sets.newHashSet();
    for (Map.Entry<PartitionRange, List<ServerNode>> assignment : assignments.entrySet()) {
      serverNodes.addAll(assignment.getValue());
    }
    assertEquals(2, serverNodes.size());
    assertTrue(serverNodes.contains(sn1));
    assertTrue(serverNodes.contains(sn2));

    // nodeNum > replica & nodeNum < shuffleNodesMax
    clusterManager.add(sn3);
    pra = strategy.assign(100, 10, 2, tags);
    assignments = pra.getAssignments();
    serverNodes = Sets.newHashSet();
    for (Map.Entry<PartitionRange, List<ServerNode>> assignment : assignments.entrySet()) {
      serverNodes.addAll(assignment.getValue());
    }
    assertEquals(3, serverNodes.size());
    assertTrue(serverNodes.contains(sn1));
    assertTrue(serverNodes.contains(sn2));
    assertTrue(serverNodes.contains(sn3));
  }
}
