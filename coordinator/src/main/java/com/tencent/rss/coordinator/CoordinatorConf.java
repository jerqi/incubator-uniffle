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

  private static final ConfigOption<String> DATA_STORAGE = ConfigOptions
      .key("rss.data.storage")
      .stringType()
      .defaultValue("local")
      .withDescription("Data storage for remote shuffle service");

  private static final ConfigOption<String> DATA_STORAGE_PATH = ConfigOptions
      .key("rss.data.storage.path")
      .stringType()
      .defaultValue("")
      .withDescription("Common storage path for remote shuffle data");

  private static final ConfigOption<String> DATA_STORAGE_PATTERN = ConfigOptions
      .key("rss.data.storage.pattern")
      .stringType()
      .defaultValue("partition")
      .withDescription("Data layout in remote shuffle service cluster");

  private static final ConfigOption<Integer> SHUFFLE_SERVER_DATA_REPLICA = ConfigOptions
      .key("rss.shuffle.data.replica")
      .intType()
      .defaultValue(2)
      .withDescription("Data replica configuration when writing into shuffle server");

  private static final ConfigOption<Integer> HEARTBEAT_INTERVAL = ConfigOptions
      .key("rss.heartbeat.interval")
      .intType()
      .defaultValue(60)
      .withDescription("Heartbeat interval (seconds) between shuffle server and coordinator");

  private static final ConfigOption<String> SHUFFLE_SERVER_ASSIGNMENT = ConfigOptions
      .key("rss.shuffle.server.assignment")
      .stringType()
      .defaultValue("basic")
      .withDescription("Strategy for assigning shuffle server to write partitions");

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

      if (SHUFFLE_SERVER_DATA_REPLICA.key().equalsIgnoreCase(k)) {
        set(SHUFFLE_SERVER_DATA_REPLICA, Integer.valueOf(v));
      }

      if (HEARTBEAT_INTERVAL.key().equalsIgnoreCase(k)) {
        set(HEARTBEAT_INTERVAL, Integer.valueOf(v));
      }

      if (SHUFFLE_SERVER_ASSIGNMENT.key().equalsIgnoreCase(k)) {
        set(SHUFFLE_SERVER_ASSIGNMENT, v);
      }

    });

    return true;
  }

  public int getHeartbeatInterval() {
    return this.getInteger(HEARTBEAT_INTERVAL);
  }

  public String getDataStorage() {
    return this.getString(DATA_STORAGE);
  }

  public String getDataStoragePath() {
    return this.getString(DATA_STORAGE_PATH);
  }

  public int getShuffleServerDataReplica() {
    return this.getInteger(SHUFFLE_SERVER_DATA_REPLICA);
  }

  public String getDataStoragePattern() {
    return this.getString(DATA_STORAGE_PATTERN);
  }

  public String getShuffleServerAssignmentStrategy() {
    return this.getString(SHUFFLE_SERVER_ASSIGNMENT);
  }
}
