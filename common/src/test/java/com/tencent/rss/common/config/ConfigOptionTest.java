package com.tencent.rss.common.config;

import org.junit.Test;

import static org.junit.Assert.*;

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

    final ConfigOption<Integer> positiveInt = ConfigOptions
        .key("key5")
        .intType()
        .checkValue((v) -> {return v > 0;}, "The value of key5 must be positive")
        .defaultValue(1)
        .withDescription("Positive integer key");
    RssBaseConf conf = new RssBaseConf();
    conf.set(positiveInt, -1);
    boolean isException = false;
    try {
      conf.get(positiveInt);
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("The value of key5 must be positive"));
    }
    assertTrue(isException);
    conf.set(positiveInt, 1);
    try {
      conf.get(positiveInt);
    } catch (IllegalArgumentException ie) {
      fail();
    }
  }
}
