package com.tencent.rss.server;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.RssUtils;
import java.util.Map;

public class ShuffleServerConf extends RssBaseConf {

  public static final String PREFIX_HADOOP_CONF = "rss.server.hadoop";

  public static final ConfigOption<Long> SERVER_BUFFER_CAPACITY = ConfigOptions
      .key("rss.server.buffer.capacity")
      .longType()
      .noDefaultValue()
      .withDescription("Max memory for shuffle server");

  public static final ConfigOption<Long> SERVER_BUFFER_SPILL_THRESHOLD = ConfigOptions
      .key("rss.server.buffer.spill.threshold")
      .longType()
      .noDefaultValue()
      .withDescription("Spill threshold for buffer manager, it must be less than rss.server.buffer.capacity");

  public static final ConfigOption<Integer> SERVER_PARTITION_BUFFER_SIZE = ConfigOptions
      .key("rss.server.partition.buffer.size")
      .intType()
      .noDefaultValue()
      .withDescription("Size of each buffer in this server");

  public static final ConfigOption<Long> SERVER_READ_BUFFER_CAPACITY = ConfigOptions
      .key("rss.server.read.buffer.capacity")
      .longType()
      .defaultValue(10000L)
      .withDescription("Size of buffer for reading data");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_DELAY = ConfigOptions
      .key("rss.server.heartbeat.delay")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("rss heartbeat initial delay ms");

  public static final ConfigOption<Integer> SERVER_HEARTBEAT_THREAD_NUM = ConfigOptions
      .key("rss.server.heartbeat.threadNum")
      .intType()
      .defaultValue(2)
      .withDescription("rss heartbeat thread number");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_INTERVAL = ConfigOptions
      .key("rss.server.heartbeat.interval")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("rss heartbeat interval ms");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_TIMEOUT = ConfigOptions
      .key("rss.server.heartbeat.timeout")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("rss heartbeat interval ms");

  public static final ConfigOption<Integer> SERVER_HEARTBEAT_MAX_FAILURE = ConfigOptions
      .key("rss.server.heartbeat.max.failure")
      .intType()
      .defaultValue(10)
      .withDescription("rss heartbeat max failure times");

  public static final ConfigOption<Integer> SERVER_FLUSH_THREAD_POOL_SIZE = ConfigOptions
      .key("rss.server.flush.threadPool.size")
      .intType()
      .defaultValue(128)
      .withDescription("thread pool for flush data to file");

  public static final ConfigOption<Integer> SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE = ConfigOptions
      .key("rss.server.flush.threadPool.queue.size")
      .intType()
      .defaultValue(Integer.MAX_VALUE)
      .withDescription("size of waiting queue for thread pool");

  public static final ConfigOption<Long> SERVER_FLUSH_THREAD_ALIVE = ConfigOptions
      .key("rss.server.flush.thread.alive")
      .longType()
      .defaultValue(120L)
      .withDescription("thread idle time in pool (s)");

  public static final ConfigOption<Long> SERVER_COMMIT_TIMEOUT = ConfigOptions
      .key("rss.server.commit.timeout")
      .longType()
      .defaultValue(120000L)
      .withDescription("Timeout when commit shuffle data (ms)");

  public static final ConfigOption<Integer> SERVER_WRITE_RETRY_MAX = ConfigOptions
      .key("rss.server.write.retry.max")
      .intType()
      .defaultValue(10)
      .withDescription("Retry times when write fail");

  public static final ConfigOption<Long> SERVER_APP_EXPIRED_WITH_HEARTBEAT = ConfigOptions
      .key("rss.server.app.expired.withHeartbeat")
      .longType()
      .defaultValue(24 * 60 * 60 * 1000L)
      .withDescription("Expired time (ms) for application which has heartbeat with coordinator");

  public static final ConfigOption<Long> SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT = ConfigOptions
      .key("rss.server.app.expired.withoutHeartbeat")
      .longType()
      .defaultValue(60 * 1000L)
      .withDescription("Expired time (ms) for application which has no heartbeat with coordinator");

  public static final ConfigOption<Integer> SERVER_MEMORY_REQUEST_RETRY_MAX = ConfigOptions
      .key("rss.server.memory.request.retry.max")
      .intType()
      .defaultValue(100)
      .withDescription("Max times to retry for memory request");

  public static final ConfigOption<Long> SERVER_PRE_ALLOCATION_EXPIRED = ConfigOptions
      .key("rss.server.preAllocation.expired")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("Expired time (ms) for pre allocated buffer");

  public static final ConfigOption<Long> SERVER_COMMIT_CHECK_INTERVAL = ConfigOptions
      .key("rss.server.commit.check.interval")
      .longType()
      .defaultValue(1000L)
      .withDescription("Interval for check commit status");

  public static final ConfigOption<Long> SERVER_WRITE_SLOW_THRESHOLD = ConfigOptions
      .key("rss.server.write.slow.threshold")
      .longType()
      .defaultValue(10000L)
      .withDescription("Threshold for write slow defined");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L1 = ConfigOptions
      .key("rss.server.event.size.threshold.l1")
      .longType()
      .defaultValue(200000L)
      .withDescription("Threshold for event size");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L2 = ConfigOptions
      .key("rss.server.event.size.threshold.l2")
      .longType()
      .defaultValue(1000000L)
      .withDescription("Threshold for event size");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L3 = ConfigOptions
      .key("rss.server.event.size.threshold.l3")
      .longType()
      .defaultValue(10000000L)
      .withDescription("Threshold for event size");

  public ShuffleServerConf() {
  }

  public ShuffleServerConf(String fileName) {
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
      if (RPC_SERVER_TYPE.key().equalsIgnoreCase(k)) {
        set(RPC_SERVER_TYPE, v.toUpperCase());
      }

      if (RPC_SERVER_PORT.key().equalsIgnoreCase(k)) {
        set(RPC_SERVER_PORT, Integer.valueOf(v));
      }

      if (SERVER_BUFFER_CAPACITY.key().equalsIgnoreCase(k)) {
        set(SERVER_BUFFER_CAPACITY, Long.valueOf(v));
      }

      if (SERVER_BUFFER_SPILL_THRESHOLD.key().equalsIgnoreCase(k)) {
        set(SERVER_BUFFER_SPILL_THRESHOLD, Long.valueOf(v));
      }

      if (SERVER_PARTITION_BUFFER_SIZE.key().equalsIgnoreCase(k)) {
        set(SERVER_PARTITION_BUFFER_SIZE, Integer.valueOf(v));
      }

      if (SERVER_HEARTBEAT_DELAY.key().equalsIgnoreCase(k)) {
        set(SERVER_HEARTBEAT_DELAY, Long.valueOf(v));
      }

      if (SERVER_HEARTBEAT_INTERVAL.key().equalsIgnoreCase(k)) {
        set(SERVER_HEARTBEAT_INTERVAL, Long.valueOf(v));
      }

      if (SERVER_HEARTBEAT_TIMEOUT.key().equalsIgnoreCase(k)) {
        set(SERVER_HEARTBEAT_TIMEOUT, Long.valueOf(v));
      }

      if (SERVER_HEARTBEAT_THREAD_NUM.key().equalsIgnoreCase(k)) {
        set(SERVER_HEARTBEAT_THREAD_NUM, Integer.valueOf(v));
      }

      if (SERVER_FLUSH_THREAD_POOL_SIZE.key().equalsIgnoreCase(k)) {
        set(SERVER_FLUSH_THREAD_POOL_SIZE, Integer.valueOf(v));
      }

      if (SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE.key().equalsIgnoreCase(k)) {
        set(SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE, Integer.valueOf(v));
      }

      if (SERVER_FLUSH_THREAD_ALIVE.key().equalsIgnoreCase(k)) {
        set(SERVER_FLUSH_THREAD_ALIVE, Long.valueOf(v));
      }

      if (SERVER_COMMIT_TIMEOUT.key().equalsIgnoreCase(k)) {
        set(SERVER_COMMIT_TIMEOUT, Long.valueOf(v));
      }

      if (SERVER_APP_EXPIRED_WITH_HEARTBEAT.key().equalsIgnoreCase(k)) {
        set(SERVER_APP_EXPIRED_WITH_HEARTBEAT, Long.valueOf(v));
      }

      if (SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT.key().equalsIgnoreCase(k)) {
        set(SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, Long.valueOf(v));
      }

      if (SERVER_MEMORY_REQUEST_RETRY_MAX.key().equalsIgnoreCase(k)) {
        set(SERVER_MEMORY_REQUEST_RETRY_MAX, Integer.valueOf(v));
      }

      if (SERVER_WRITE_RETRY_MAX.key().equalsIgnoreCase(k)) {
        set(SERVER_WRITE_RETRY_MAX, Integer.valueOf(v));
      }

      if (SERVER_PRE_ALLOCATION_EXPIRED.key().equalsIgnoreCase(k)) {
        set(SERVER_PRE_ALLOCATION_EXPIRED, Long.valueOf(v));
      }

      if (SERVER_COMMIT_CHECK_INTERVAL.key().equalsIgnoreCase(k)) {
        set(SERVER_COMMIT_CHECK_INTERVAL, Long.valueOf(v));
      }

      if (SERVER_READ_BUFFER_CAPACITY.key().equalsIgnoreCase(k)) {
        set(SERVER_READ_BUFFER_CAPACITY, Long.valueOf(v));
      }

      if (SERVER_WRITE_SLOW_THRESHOLD.key().equalsIgnoreCase(k)) {
        set(SERVER_WRITE_SLOW_THRESHOLD, Long.valueOf(v));
      }

      if (SERVER_EVENT_SIZE_THRESHOLD_L1.key().equalsIgnoreCase(k)) {
        set(SERVER_EVENT_SIZE_THRESHOLD_L1, Long.valueOf(v));
      }

      if (SERVER_EVENT_SIZE_THRESHOLD_L2.key().equalsIgnoreCase(k)) {
        set(SERVER_EVENT_SIZE_THRESHOLD_L2, Long.valueOf(v));
      }

      if (SERVER_EVENT_SIZE_THRESHOLD_L3.key().equalsIgnoreCase(k)) {
        set(SERVER_EVENT_SIZE_THRESHOLD_L3, Long.valueOf(v));
      }

      if (k.startsWith(PREFIX_HADOOP_CONF)) {
        setString(k, v);
      }

    });

    return true;
  }
}
