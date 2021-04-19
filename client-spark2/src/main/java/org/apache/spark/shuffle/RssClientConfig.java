package org.apache.spark.shuffle;

public class RssClientConfig {

  public static String RSS_PARTITION_NUM_PER_RANGE = "spark.rss.partitionNum.per.range";
  public static int RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE = 1;
  public static String RSS_WRITER_BUFFER_SIZE = "spark.rss.writer.buffer.size";
  public static String RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE = "8m";
  public static String RSS_WRITER_SERIALIZER_BUFFER_SIZE = "spark.rss.writer.serializer.buffer.size";
  public static String RSS_WRITER_SERIALIZER_BUFFER_SIZE_DEFAULT_VALUE = "1m";
  public static String RSS_WRITER_BUFFER_SEGMENT_SIZE = "spark.rss.writer.buffer.segment.size";
  public static String RSS_WRITER_BUFFER_SEGMENT_SIZE_DEFAULT_VALUE = "3k";
  public static String RSS_WRITER_BUFFER_SPILL_SIZE = "spark.rss.writer.buffer.spill.size";
  public static String RSS_WRITER_BUFFER_SPILL_SIZE_DEFAULT_VALUE = "256m";
  public static String RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE = "spark.rss.writer.pre.allocated.buffer.size";
  public static String RSS_WRITER_PRE_ALLOCATED_BUFFER_SIZE_DEFAULT_VALUE = "16m";
  public static String RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX = "spark.rss.writer.require.memory.retryMax";
  public static int RSS_WRITER_REQUIRE_MEMORY_RETRY_MAX_DEFAULT_VALUE = 120;
  public static String RSS_WRITER_REQUIRE_MEMORY_INTERVAL = "spark.rss.writer.require.memory.interval";
  public static long RSS_WRITER_REQUIRE_MEMORY_INTERVAL_DEFAULT_VALUE = 1000; // 1s
  public static String RSS_COORDINATOR_IP = "spark.rss.coordinator.ip";
  public static String RSS_COORDINATOR_PORT = "spark.rss.coordinator.port";
  public static String RSS_WRITER_SEND_CHECK_TIMEOUT = "spark.rss.writer.send.check.timeout";
  public static long RSS_WRITER_SEND_CHECK_TIMEOUT_DEFAULT_VALUE = 2 * 60 * 1000; // 2 min
  public static String RSS_WRITER_SEND_CHECK_INTERVAL = "spark.rss.writer.send.check.interval";
  public static long RSS_WRITER_SEND_CHECK_INTERVAL_DEFAULT_VALUE = 1000;
  public static String RSS_TEST_FLAG = "spark.rss.test";
  public static String RSS_BASE_PATH = "spark.rss.base.path";
  public static String RSS_INDEX_READ_LIMIT = "spark.rss.index.read.limit";
  public static int RSS_INDEX_READ_LIMIT_DEFAULT_VALUE = 1000;
  public static String RSS_CLIENT_TYPE = "spark.rss.client.type";
  public static String RSS_CLIENT_TYPE_DEFAULT_VALUE = "GRPC";
  public static String RSS_STORAGE_TYPE = "spark.rss.storage.type";
  public static String RSS_CLIENT_RETRY_MAX = "spark.rss.client.retry.max";
  public static int RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE = 30;
  public static String RSS_CLIENT_RETRY_INTERVAL_MAX = "spark.rss.client.retry.interval.max";
  public static long RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE = 10000;
  public static String RSS_CLIENT_SEND_SIZE_LIMIT = "spark.rss.client.send.size.limit";
  public static String RSS_CLIENT_SEND_SIZE_LIMIT_DEFAULT_VALUE = "32m";
  public static String RSS_CLIENT_READ_BUFFER_SIZE = "spark.rss.client.read.buffer.size";
  public static String RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE = "64m";
  public static String RSS_HEARTBEAT_INTERVAL = "spark.rss.heartbeat.interval";
  public static long RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE = 10 * 1000L;
  public static String RSS_CLIENT_SEND_THREAD_POOL_SIZE = "spark.rss.client.send.threadPool.size";
  public static int RSS_CLIENT_SEND_THREAD_POOL_SIZE_DEFAULT_VALUE = 24;
  public static String RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE = "spark.rss.client.send.threadPool.keepalive";
  public static int RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE_DEFAULT_VALUE = 60;
  public static String RSS_DATA_REPLICA = "spark.rss.data.replica";
  public static int RSS_DATA_REPLICA_DEFAULT_VALUE = 1;
}
