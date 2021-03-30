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

  static final ConfigOption<Long> COORDINATOR_HEARTBEAT_TIMEOUT = ConfigOptions
      .key("rss.coordinator.server.heartbeat.timeout")
      .longType()
      .defaultValue(30 * 1000L)
      .withDescription("timeout if can't get heartbeat from shuffle server");

  static final ConfigOption<String> COORDINATOR_ASSIGNMENT_STRATEGY = ConfigOptions
      .key("rss.coordinator.assignment.strategy")
      .stringType()
      .defaultValue("BASIC")
      .withDescription("Strategy for assigning shuffle server to write partitions");

  static final ConfigOption<Long> COORDINATOR_APP_EXPIRED = ConfigOptions
      .key("rss.coordinator.app.expired")
      .longType()
      .defaultValue(60 * 1000L)
      .withDescription("Application expired time (ms), the heartbeat interval must be less than it");

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

      if (COORDINATOR_ASSIGNMENT_STRATEGY.key().equalsIgnoreCase(k)) {
        set(COORDINATOR_ASSIGNMENT_STRATEGY, v.toUpperCase());
      }

      if (COORDINATOR_HEARTBEAT_TIMEOUT.key().equalsIgnoreCase(k)) {
        set(COORDINATOR_HEARTBEAT_TIMEOUT, Long.valueOf(v));
      }

      if (COORDINATOR_APP_EXPIRED.key().equalsIgnoreCase(k)) {
        set(COORDINATOR_APP_EXPIRED, Long.valueOf(v));
      }
    });

    return true;
  }
}
