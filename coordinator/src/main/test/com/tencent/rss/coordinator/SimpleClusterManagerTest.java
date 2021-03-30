package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.junit.Test;

public class SimpleClusterManagerTest {

  @Test
  public void getServerListTest() {
    SimpleClusterManager clusterManager = new SimpleClusterManager(30 * 1000L);
    ServerNode sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10);
    ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10);
    ServerNode sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11);
    ServerNode sn4 = new ServerNode("sn4", "ip", 0, 100L, 51L, 20, 10);
    clusterManager.add(sn1);
    clusterManager.add(sn2);
    clusterManager.add(sn3);
    clusterManager.add(sn4);
    List<ServerNode> serverNodes = clusterManager.getServerList(1);
    assertEquals(1, serverNodes.size());
    assertEquals("sn2", serverNodes.get(0).getId());
    serverNodes = clusterManager.getServerList(4);
    assertEquals(4, serverNodes.size());
    assertEquals("sn2", serverNodes.get(0).getId());
    assertEquals("sn3", serverNodes.get(1).getId());
    assertEquals("sn1", serverNodes.get(2).getId());
    assertEquals("sn4", serverNodes.get(3).getId());
    serverNodes = clusterManager.getServerList(5);
    assertEquals(4, serverNodes.size());
    assertEquals("sn2", serverNodes.get(0).getId());
    assertEquals("sn3", serverNodes.get(1).getId());
    assertEquals("sn1", serverNodes.get(2).getId());
    assertEquals("sn4", serverNodes.get(3).getId());
  }

  @Test
  public void heartbeatTimeoutTest() throws Exception {
    SimpleClusterManager clusterManager = new SimpleClusterManager(3 * 1000L);
    Thread t = new Thread(() -> {
      for (int i = 0; i < 3; i++) {
        if (i < 1) {
          ServerNode sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10);
          clusterManager.add(sn1);
        }
        ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10);
        clusterManager.add(sn2);
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
        }
      }
    });
    t.start();
    List<ServerNode> serverNodes = clusterManager.getServerList(2);
    assertEquals(2, serverNodes.size());
    assertEquals("sn2", serverNodes.get(0).getId());
    assertEquals("sn1", serverNodes.get(1).getId());
    Thread.sleep(5000);
    serverNodes = clusterManager.getServerList(2);
    assertEquals(1, serverNodes.size());
    assertEquals("sn2", serverNodes.get(0).getId());
    Thread.sleep(5000);
    serverNodes = clusterManager.getServerList(2);
    assertEquals(0, serverNodes.size());
  }
}
