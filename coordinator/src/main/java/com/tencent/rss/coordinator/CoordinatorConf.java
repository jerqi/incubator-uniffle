package com.tencent.rss.coordinator;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.RssUtils;
import java.util.Map;

/**
 * Configuration for Coordinator Service and rss-cluster, including service port,
 * heartbeat interval and etc.
 */
public class CoordinatorConf extends RssBaseConf {

  static final ConfigOption<Long> ALIVE_THRESHOLD = ConfigOptions
      .key("rss.coordinator.aliveThreshold")
      .longType()
      .defaultValue(5 * 60 * 1000L)
      .withDescription("rss coordinator aliveThreshold");
  static final ConfigOption<Integer> USABLE_THRESHOLD = ConfigOptions
      .key("rss.coordinator.usableThreshold")
      .intType()
      .defaultValue(10)
      .withDescription("rss coordinator usableThreshold");
  static final ConfigOption<Integer> SHUFFLE_SERVER_REPLICA = ConfigOptions
      .key("rss.coordinator.server.replica")
      .intType()
      .defaultValue(2)
      .withDescription("Data replica configuration when writing into shuffle server");
  static final ConfigOption<String> ASSIGNMENT_STRATEGY = ConfigOptions
      .key("rss.coordinator.assignment.strategy")
      .stringType()
      .defaultValue("BASIC")
      .withDescription("Strategy for assigning shuffle server to write partitions");
  private static final ConfigOption<String> DATA_STORAGE = ConfigOptions
      .key("rss.storage.type")
      .stringType()
      .defaultValue("local")
      .withDescription("Data storage for remote shuffle service");
  private static final ConfigOption<String> DATA_STORAGE_PATH = ConfigOptions
      .key("rss.storage.path")
      .stringType()
      .defaultValue("")
      .withDescription("Common storage path for remote shuffle data");
  private static final ConfigOption<String> DATA_STORAGE_PATTERN = ConfigOptions
      .key("rss.storage.pattern")
      .stringType()
      .defaultValue("partition")
      .withDescription("Data layout in remote shuffle service cluster");

  public CoordinatorConf() {
  }

  public CoordinatorConf(String fileName) {
    super();
    boolean ret = loadConfFromFile(fileName);
    if (!ret) {
      throw new IllegalStateException("Fail to load config file " + fileName);
    }
  }

  public boolean loadConfFromFile(String fileName) {
    Map<String, String> properties = RssUtils.getPropertiesFromFile(fileName);

    if (properties == null) {
      return false;
    }

    loadCommonConf(properties);

    properties.forEach((k, v) -> {
      if (DATA_STORAGE.key().equalsIgnoreCase(k)) {
        set(DATA_STORAGE, v.toUpperCase());
      }

      if (DATA_STORAGE_PATH.key().equalsIgnoreCase(k)) {
        set(DATA_STORAGE_PATH, v);
      }

      if (DATA_STORAGE_PATTERN.key().equalsIgnoreCase(k)) {
        set(DATA_STORAGE_PATTERN, v);
      }

      if (SHUFFLE_SERVER_REPLICA.key().equalsIgnoreCase(k)) {
        set(SHUFFLE_SERVER_REPLICA, Integer.valueOf(v));
      }

      if (ASSIGNMENT_STRATEGY.key().equalsIgnoreCase(k)) {
        set(ASSIGNMENT_STRATEGY, v.toUpperCase());
      }

      if (ALIVE_THRESHOLD.key().equalsIgnoreCase(k)) {
        set(ALIVE_THRESHOLD, Long.valueOf(v));
      }

      if (USABLE_THRESHOLD.key().equalsIgnoreCase(k)) {
        set(USABLE_THRESHOLD, Integer.valueOf(v));
      }

    });

    return true;
  }

  public String getDataStorage() {
    return this.getString(DATA_STORAGE);
  }

  public String getDataStoragePath() {
    return this.getString(DATA_STORAGE_PATH);
  }

  public int getShuffleServerReplica() {
    return this.getInteger(SHUFFLE_SERVER_REPLICA);
  }

  public String getDataStoragePattern() {
    return this.getString(DATA_STORAGE_PATTERN);
  }

  public String getShuffleServerAssignmentStrategy() {
    return this.getString(ASSIGNMENT_STRATEGY);
  }
}
