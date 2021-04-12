package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class ServerNodeTest {

  @Test
  public void valueOftest() {
    ShuffleServerHeartBeatRequest request =
        ShuffleServerHeartBeatRequest
            .newBuilder()
            .setServerId(ShuffleServerId.newBuilder().setIp("ip").setPort(9001).setId("id").build())
            .setUsedMemory(100L)
            .setPreAllocatedMemory(101L)
            .setAvailableMemory(102L)
            .setEventNumInFlush(1)
            .build();
    ServerNode info = ServerNode.valueOf(request);
    assertEquals("ip", info.getIp());
    assertEquals(9001, info.getPort());
    assertEquals("id", info.getId());
    assertEquals(100, info.getUsedMemory());
    assertEquals(101, info.getPreAllocatedMemory());
    assertEquals(102, info.getAvailableMemory());
    assertEquals(1, info.getEventNumInFlush());
  }

  @Test
  public void compareTest() {
    ServerNode sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20, 10);
    ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21, 10);
    ServerNode sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20, 11);
    ServerNode sn4 = new ServerNode("sn4", "ip", 0, 100L, 51L, 20, 10);
    List<ServerNode> nodes = Lists.newArrayList(sn1, sn2, sn3, sn4);
    Collections.sort(nodes);
    assertEquals("sn2", nodes.get(0).getId());
    assertEquals("sn3", nodes.get(1).getId());
    assertEquals("sn1", nodes.get(2).getId());
    assertEquals("sn4", nodes.get(3).getId());
  }
}
