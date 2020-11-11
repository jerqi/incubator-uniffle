package com.tencent.rss.common.config;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RssConfTest {
    @Test
    public void testOptionWithDefault() {
        RssConf cfg = new RssConf();
        cfg.setInteger("int-key", 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption = ConfigOptions
                .key("string-key")
                .stringType()
                .defaultValue("my-beautiful-default");
        ConfigOption<Integer> presentIntOption = ConfigOptions
                .key("int-key")
                .intType()
                .defaultValue(87);

        assertEquals("abc", cfg.getString(presentStringOption));
        assertEquals("abc", cfg.getValue(presentStringOption));

        assertEquals(11, cfg.getInteger(presentIntOption));
        assertEquals("11", cfg.getValue(presentIntOption));
    }

    @Test
    public void testOptionWithNoDefault() {
        RssConf cfg = new RssConf();
        cfg.setInteger("int-key", 11);
        cfg.setString("string-key", "abc");

        ConfigOption<String> presentStringOption = ConfigOptions
                .key("string-key")
                .stringType()
                .noDefaultValue();

        assertEquals("abc", cfg.getString(presentStringOption));
        assertEquals("abc", cfg.getValue(presentStringOption));

        // test getting default when no value is present

        ConfigOption<String> stringOption = ConfigOptions
                .key("test")
                .stringType()
                .noDefaultValue();

        // getting strings for null should work
        assertNull(cfg.getValue(stringOption));
        assertNull(cfg.getString(stringOption));

        // overriding the null default should work
        assertEquals("override", cfg.getString(stringOption, "override"));
    }


}
