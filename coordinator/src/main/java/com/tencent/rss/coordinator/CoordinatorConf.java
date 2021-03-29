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

  static final ConfigOption<String> ASSIGNMENT_STRATEGY = ConfigOptions
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

      if (ASSIGNMENT_STRATEGY.key().equalsIgnoreCase(k)) {
        set(ASSIGNMENT_STRATEGY, v.toUpperCase());
      }

      if (ALIVE_THRESHOLD.key().equalsIgnoreCase(k)) {
        set(ALIVE_THRESHOLD, Long.valueOf(v));
      }

      if (USABLE_THRESHOLD.key().equalsIgnoreCase(k)) {
        set(USABLE_THRESHOLD, Integer.valueOf(v));
      }

      if (COORDINATOR_APP_EXPIRED.key().equalsIgnoreCase(k)) {
        set(COORDINATOR_APP_EXPIRED, Long.valueOf(v));
      }
    });

    return true;
  }

  public String getShuffleServerAssignmentStrategy() {
    return this.getString(ASSIGNMENT_STRATEGY);
  }
}
