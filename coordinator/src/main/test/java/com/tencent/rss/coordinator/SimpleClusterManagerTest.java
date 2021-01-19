package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleClusterManagerTest {

  private final SimpleClusterManager clusterManager = new SimpleClusterManager(1000L, 50);

  @Before
  public void setUp() {

  }

  @After
  public void tearDown() {

  }

  @Test
  public void test() {
    for (int i = 0; i < 100; ++i) {
      clusterManager.add(new ServerNode(String.valueOf(i), String.valueOf(i), i, i));
    }

    List<ServerNode> nodes = clusterManager.get(20);
    for (int i = 0; i < 20; ++i) {
      assertEquals(99 - i, nodes.get(i).getScore());
    }

    nodes = clusterManager.get(51);
    assertEquals(50, nodes.size());

  }

}
