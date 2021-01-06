package com.tencent.rss.common.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ConfigOptionTest {

  @Test
  public void testBasicTypes() {
    final ConfigOption<Integer> intConfig = ConfigOptions
        .key("rss.key1")
        .intType()
        .defaultValue(1000)
        .withDescription("Int config key1");
    assertSame(Integer.class, intConfig.getClazz());
    assertEquals(1000, (int) intConfig.defaultValue());
    assertEquals("Int config key1", intConfig.description());

    final ConfigOption<Long> longConfig = ConfigOptions
        .key("rss.key2")
        .longType()
        .defaultValue(1999L);
    assertTrue(longConfig.hasDefaultValue());
    assertEquals(1999L, (long) longConfig.defaultValue());

    final ConfigOption<String> stringConfig = ConfigOptions
        .key("rss.key3")
        .stringType()
        .noDefaultValue();
    assertFalse(stringConfig.hasDefaultValue());
    assertEquals("", stringConfig.description());

    final ConfigOption<Boolean> booleanConfig = ConfigOptions
        .key("key4")
        .booleanType()
        .defaultValue(false)
        .withDescription("Boolean config key");
    assertFalse(booleanConfig.defaultValue());
    assertEquals("Boolean config key", booleanConfig.description());
  }
}
