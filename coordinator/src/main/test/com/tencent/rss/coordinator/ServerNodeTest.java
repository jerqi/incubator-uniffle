package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;

import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import org.junit.Test;

public class ServerNodeTest {

  @Test
  public void test() {
    ShuffleServerHeartBeatRequest request =
        ShuffleServerHeartBeatRequest
            .newBuilder()
            .setServerId(ShuffleServerId.newBuilder().setIp("ip").setPort(9001).setId("id").build())
            .setScore(95)
            .build();
    ServerNode info = ServerNode.valueOf(request);
    assertEquals("ip", info.getIp());
    assertEquals(9001, info.getPort());
    assertEquals("id", info.getId());
    assertEquals(95, info.getScore());
  }
}
