package com.tencent.rss.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Objects;
import org.junit.jupiter.api.Test;

public class RssUtilsTest {
    @Test
    public void testGetPropertiesFromFile() {
        final String filePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource("rss-defaults.conf")).getFile();
        Map<String, String> properties = RssUtils.getPropertiesFromFile(filePath);
        assertEquals("12121", properties.get("com.tencent.rss.coordinator.port"));
        assertEquals("155", properties.get("com.tencent.rss.heartbeat.interval"));
        assertEquals("true", properties.get("rss.x.y.z"));
        assertEquals("-XX:+PrintGCDetails -Dkey=value -Dnumbers=\"one two three\"", properties.get("rss.a.b.c.extraJavaOptions"));
    }

}
