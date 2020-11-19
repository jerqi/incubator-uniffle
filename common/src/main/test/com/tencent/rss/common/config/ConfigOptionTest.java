package com.tencent.rss.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

public class ConfigOptionTest {
    @Test
    public void testBasicTypes() {
        final ConfigOption<Integer> intConfig = ConfigOptions
                .key("com.tencent.rss.key1")
                .intType()
                .defaultValue(1000)
                .withDescription("Int config key1");
        assertSame(Integer.class, intConfig.getClazz());
        assertEquals(1000, (int) intConfig.defaultValue());
        assertEquals("Int config key1", intConfig.description());

        final ConfigOption<Long> longConfig = ConfigOptions
                .key("com.tencent.rss.key2")
                .longType()
                .defaultValue(1999L);
        assertTrue(longConfig.hasDefaultValue());
        assertEquals(1999L, (long) longConfig.defaultValue());

        final ConfigOption<String> stringConfig = ConfigOptions
                .key("com.tencent.rss.key3")
                .stringType()
                .noDefaultValue();
        assertFalse(stringConfig.hasDefaultValue());
        assertEquals("", stringConfig.description());

        final ConfigOption<Boolean> booleanConfig = ConfigOptions
                .key("com.tencent.key4")
                .booleanType()
                .defaultValue(false)
                .withDescription("Boolean config key");
        assertFalse(booleanConfig.defaultValue());
        assertEquals("Boolean config key", booleanConfig.description());
    }
}
