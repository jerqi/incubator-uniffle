package com.tencent.rss.coordinator;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.RssBaseConf;

/**
 * Configuration for Coordinator Service and rss-cluster, including service port,
 * heartbeat interval and etc.
 */
public class CoordinatorConf extends RssBaseConf {

  private static final ConfigOption<Integer> SERVICE_PORT = ConfigOptions
      .key("com.tencent.rss.coordinator.port")
      .intType()
      .defaultValue(19999)
      .withDescription("Coordinator service port");

  private static final ConfigOption<String> DATA_STORAGE = ConfigOptions
      .key("com.tencent.rss.data.storage")
      .stringType()
      .defaultValue("local")
      .withDescription("Data storage for remote shuffle service");

  private static final ConfigOption<String> DATA_STORAGE_PATH = ConfigOptions
      .key("com.tencent.rss.data.storage.path")
      .stringType()
      .defaultValue("")
      .withDescription("Common storage path for remote shuffle data");

  private static final ConfigOption<String> DATA_STORAGE_PATTERN = ConfigOptions
      .key("com.tencent.rss.data.storage.pattern")
      .stringType()
      .defaultValue("partition")
      .withDescription("Data layout in remote shuffle service cluster");

  private static final ConfigOption<Integer> SHUFFLE_SERVER_DATA_REPLICA = ConfigOptions
      .key("com.tencent.rss.shuffle.data.replica")
      .intType()
      .defaultValue(2)
      .withDescription("Data replica configuration when writing into shuffle server");

  private static final ConfigOption<Integer> HEARTBEAT_INTERVAL = ConfigOptions
      .key("com.tencent.rss.heartbeat.interval")
      .intType()
      .defaultValue(60)
      .withDescription("Heartbeat interval (seconds) between shuffle server and coordinator");

  private static final ConfigOption<String> SHUFFLE_SERVER_ASSIGNMENT = ConfigOptions
      .key("com.tencent.rss.shuffle.server.assignment")
      .stringType()
      .defaultValue("basic")
      .withDescription("Strategy for assigning shuffle server to write partitions");


  public int getCoordinatorServicePort() {
    return this.getInteger(SERVICE_PORT);
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
