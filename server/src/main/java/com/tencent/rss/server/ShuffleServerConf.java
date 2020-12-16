package com.tencent.rss.server;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.RssConf;
import com.tencent.rss.common.util.RssUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ShuffleServerConf extends RssConf {

  public static final ConfigOption<Integer> SERVICE_PORT = ConfigOptions
    .key("rss.server.port")
    .intType()
    .noDefaultValue()
    .withDescription("Shuffle server service port");
  public static final ConfigOption<String> DATA_STORAGE_TYPE = ConfigOptions
    .key("rss.storage.type")
    .stringType()
    .noDefaultValue()
    .withDescription("Data storage for remote shuffle service");
  public static final ConfigOption<String> DATA_STORAGE_BASE_PATH = ConfigOptions
    .key("rss.data.storage.basePath")
    .stringType()
    .noDefaultValue()
    .withDescription("Common storage path for remote shuffle data");
  public static final ConfigOption<Integer> BUFFER_CAPACITY = ConfigOptions
    .key("rss.buffer.capacity")
    .intType()
    .noDefaultValue()
    .withDescription("Number of buffers in this server");
  public static final ConfigOption<Integer> BUFFER_SIZE = ConfigOptions
    .key("rss.buffer.size")
    .intType()
    .noDefaultValue()
    .withDescription("Size of each buffer in this server");

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerConf.class);

  public boolean loadConfFromFile(String fileName) {
    Map<String, String> properties = RssUtils.getPropertiesFromFile(fileName);
    if (properties == null) {
      return false;
    }

    properties.forEach((k, v) -> {
      if (SERVICE_PORT.key().equalsIgnoreCase(k)) {
        set(SERVICE_PORT, Integer.valueOf(v));
      }

      if (DATA_STORAGE_TYPE.key().equalsIgnoreCase(k)) {
        set(DATA_STORAGE_TYPE, v.toUpperCase());
      }

      if (DATA_STORAGE_BASE_PATH.key().equalsIgnoreCase(k)) {
        set(DATA_STORAGE_BASE_PATH, v);
      }

      if (BUFFER_CAPACITY.key().equalsIgnoreCase(k)) {
        set(BUFFER_CAPACITY, Integer.valueOf(v));
      }

      if (BUFFER_SIZE.key().equalsIgnoreCase(k)) {
        set(BUFFER_SIZE, Integer.valueOf(v));
      }
    });

    return true;
  }
}
