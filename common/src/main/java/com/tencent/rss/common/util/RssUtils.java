package com.tencent.rss.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(RssUtils.class);

    /** Load properties present in the given file. */
    public static Map<String, String> getPropertiesFromFile(String filename) {
        File file = new File(filename);
        final Map<String, String> result = new HashMap<>();
        if (!file.exists()) {
            LOGGER.error("Properties file " + filename + " does not exist");
            return result;
        }
        if (!file.isFile()) {
            LOGGER.error("Properties file " + filename + " is not a normal file");
            return result;
        }
        try (InputStreamReader inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
            Properties properties = new Properties();
            properties.load(inReader);
            properties.stringPropertyNames().forEach(k -> result.put(k, properties.getProperty(k).trim()));
        } catch (IOException ignored) {
            LOGGER.error("Failed when loading rss properties from " + filename);
        }
        return result;
    }
}
