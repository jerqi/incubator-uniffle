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

  /**
   * Load properties present in the given file.
   */
  public static Map<String, String> getPropertiesFromFile(String filename) {
    if (filename == null) {
      String rssHome = System.getenv("RSS_HOME");
      if (rssHome == null) {
        LOGGER.error("Both conf file and RSS_HOME env is null");
        return null;
      }

      LOGGER.info("Conf file is null use {}'s server.conf", rssHome);
      filename = rssHome + "/server.conf";
    }

    File file = new File(filename);

    if (!file.exists()) {
      LOGGER.error("Properties file " + filename + " does not exist");
      return null;
    }

    if (!file.isFile()) {
      LOGGER.error("Properties file " + filename + " is not a normal file");
      return null;
    }

    LOGGER.info("Load config from {}", filename);
    final Map<String, String> result = new HashMap<>();

    try (InputStreamReader inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
      Properties properties = new Properties();
      properties.load(inReader);
      properties.stringPropertyNames().forEach(k -> result.put(k, properties.getProperty(k).trim()));
    } catch (IOException ignored) {
      LOGGER.error("Failed when loading rss properties from " + filename);
    }

    return result;
  }

  public static String getShuffleDataPath(String appId, String shuffleId, int start, int end) {
    return String.join(
        "_",
        appId,
        String.valueOf(shuffleId),
        String.join("-", String.valueOf(start), String.valueOf(end)));
  }

  public static String getFullShuffleDataFolder(String basePath, String subPath) {
    return String.join("/", basePath, subPath);
  }
}
