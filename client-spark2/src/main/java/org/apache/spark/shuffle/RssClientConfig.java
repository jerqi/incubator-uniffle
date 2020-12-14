package org.apache.spark.shuffle;

public class RssClientConfig {

    public static String RSS_PARTITIONS_PER_SERVER = "spark.rss.partitions.per.server";
    public static int RSS_PARTITIONS_PER_SERVER_DEFAULT_VALUE = 5;
    public static String RSS_WRITER_BUFFER_SIZE = "spark.rss.writer.buffer.size";
    public static int RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE = 2097152; // 1024 * 1024 * 2 = 2M
    public static String RSS_WRITER_BUFFER_MAX_SIZE = "spark.rss.writer.buffer.max.size";
    public static int RSS_WRITER_BUFFER_MAX_SIZE_DEFAULT_VALUE = RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE * 2;
    public static String RSS_WRITER_BUFFER_SPILL_SIZE = "spark.rss.writer.buffer.spill.size";
    public static int RSS_WRITER_BUFFER_SPILL_SIZE_DEFAULT_VALUE = RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE * 32;
    public static String RSS_COORDINATOR_IP = "spark.rss.coordinator.ip";
    public static String RSS_COORDINATOR_PORT = "spark.rss.coordinator.port";
    public static int RSS_COORDINATOR_PORT_DEFAULT_VALUE = 17777;
    public static String RSS_WRITER_SEND_CHECK_TIMEOUT = "spark.rss.writer.send.check.timeout";
    public static long RSS_WRITER_SEND_CHECK_TIMEOUT_DEFAULT_VALUE = 10 * 60 * 1000; // 10 min
    public static String RSS_WRITER_SEND_CHECK_INTERVAL = "spark.rss.writer.send.check.interval";
    public static long RSS_WRITER_SEND_CHECK_INTERVAL_DEFAULT_VALUE = 1000;
    public static String RSS_TEST_FLAG = "spark.rss.test";
    public static String RSS_BASE_PATH = "spark.rss.base.path";
    public static String RSS_INDEX_READ_LIMIT = "spark.rss.index.read.limit";
    public static int RSS_INDEX_READ_LIMIT_DEFAULT_VALUE = 1000;
}
