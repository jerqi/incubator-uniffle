package com.tencent.rss.coordinator;

/**
 * Configuration for Coordinator Service and rss-cluster, including service port,
 * heartbeat interval and etc.
 * TODO: refactor and combine this class with {@code RssConf}
 */
public class CoordinatorConf {
    public static final String COORDINATOR_PORT = "com.tencent.rss.coordinator.port";
    public static final String RSS_STORAGE = "com.tencent.rss.data.storage";
    public static final String RSS_STORAGE_PREFIX = "com.tencent.rss.data.storage.prefix";
    public static final String RSS_STORAGE_PATTERN = "com.tencent.rss.data.pattern";
    public static final String RSS_HEART_BEAT_INTERVAL = "com.tencent.rss.heartbeat.interval";

    public int getCoordinatorServicePort() {
        return 19999;
    }

    public int getHeartbeatInterval() {
        return 180;
    }

    public String getDataStorage() {
        return "hdfs";
    }

    public String getDataStoragePrefix() {
        return "hdfs://xxx//yyy";
    }

    public String getDataStoragePattern() {
        return "partition";
    }
}
