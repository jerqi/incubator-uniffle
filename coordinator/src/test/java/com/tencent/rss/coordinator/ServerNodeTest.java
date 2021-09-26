package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class ServerNodeTest {

  @Test
  public void compareTest() {
    Set<String> tags = Sets.newHashSet("test");
    ServerNode sn1 = new ServerNode("sn1", "ip", 0, 100L, 50L, 20,
        10, tags);
    ServerNode sn2 = new ServerNode("sn2", "ip", 0, 100L, 50L, 21,
        10, tags);
    ServerNode sn3 = new ServerNode("sn3", "ip", 0, 100L, 50L, 20,
        11, tags);
    List<ServerNode> nodes = Lists.newArrayList(sn1, sn2, sn3);
    Collections.sort(nodes);
    assertEquals("sn2", nodes.get(0).getId());
    assertEquals("sn1", nodes.get(1).getId());
    assertEquals("sn3", nodes.get(2).getId());
  }
}
