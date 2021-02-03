package com.tencent.rss.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Objects;
import org.junit.Test;

public class RssUtilsTest {

  @Test
  public void testGetPropertiesFromFile() {
    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("rss-defaults.conf")).getFile();
    Map<String, String> properties = RssUtils.getPropertiesFromFile(filePath);
    assertEquals("12121", properties.get("rss.server.coordinator.port"));
    assertEquals("155", properties.get("rss.server.heartbeat.interval"));
    assertEquals("true", properties.get("rss.x.y.z"));
    assertEquals("-XX:+PrintGCDetails-Dkey=value-Dnumbers=\"one two three\"",
        properties.get("rss.a.b.c.extraJavaOptions"));
  }

  @Test
  public void getShuffleDataPathWithRangeTest() {
    String result = RssUtils.getShuffleDataPathWithRange("appId", 0, 1, 3, 6);
    assertEquals("appId/0/0-2", result);
    result = RssUtils.getShuffleDataPathWithRange("appId", 0, 2, 3, 6);
    assertEquals("appId/0/0-2", result);
    result = RssUtils.getShuffleDataPathWithRange("appId", 0, 3, 3, 6);
    assertEquals("appId/0/3-5", result);
    result = RssUtils.getShuffleDataPathWithRange("appId", 0, 5, 3, 6);
    assertEquals("appId/0/3-5", result);
    try {
      RssUtils.getShuffleDataPathWithRange("appId", 0, 6, 3, 6);
      fail("shouldn't be here");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Can't generate ShuffleData Path"));
    }
    result = RssUtils.getShuffleDataPathWithRange("appId", 0, 6, 3, 7);
    assertEquals("appId/0/6-8", result);
  }
}
