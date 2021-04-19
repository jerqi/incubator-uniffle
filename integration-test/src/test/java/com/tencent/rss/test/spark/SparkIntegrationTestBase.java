package com.tencent.rss.test.spark;

import static org.junit.Assert.assertEquals;

import com.tencent.rss.test.IntegrationTestBase;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class SparkIntegrationTestBase extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SparkIntegrationTestBase.class);

  abstract Map runTest(SparkSession spark, String fileName) throws Exception;

  public String generateTestFile() throws Exception {
    return null;
  }

  public void updateSparkConfCustomer(SparkConf sparkConf) {
  }

  public void run() throws Exception {

    String fileName = generateTestFile();
    SparkConf sparkConf = createSparkConf();

    long start = System.currentTimeMillis();
    Map resultWithoutRss = runSparkApp(sparkConf, fileName);
    long durationWithoutRss = System.currentTimeMillis() - start;

    updateSparkConfWithRss(sparkConf);
    updateSparkConfCustomer(sparkConf);
    start = System.currentTimeMillis();
    Map resultWithRss = runSparkApp(sparkConf, fileName);
    long durationWithRss = System.currentTimeMillis() - start;

    verifyTestResult(resultWithoutRss, resultWithRss);

    LOG.info("Test: durationWithoutRss[" + durationWithoutRss
        + "], durationWithRss[" + durationWithRss + "]");
  }

  protected Map runSparkApp(SparkConf sparkConf, String testFileName) throws Exception {
    SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
    Map resultWithRss = runTest(spark, testFileName);
    spark.stop();
    return resultWithRss;
  }

  protected SparkConf createSparkConf() {
    return new SparkConf()
        .setAppName(this.getClass().getSimpleName())
        .setMaster("local[4]");
  }

  public void updateSparkConfWithRss(SparkConf sparkConf) {
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.rss.partitions.per.range", "2");
    sparkConf.set(RssClientConfig.RSS_WRITER_BUFFER_SIZE, "4m");
    sparkConf.set(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE, "32m");
    sparkConf.set(RssClientConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE, "128k");
    sparkConf.set(RssClientConfig.RSS_WRITER_BUFFER_SEGMENT_SIZE, "256k");
    sparkConf.set(RssClientConfig.RSS_COORDINATOR_IP, LOCALHOST);
    sparkConf.set(RssClientConfig.RSS_COORDINATOR_PORT, "" + COORDINATOR_PORT);
    sparkConf.set(RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT, "30000");
    sparkConf.set(RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL, "1000");
    sparkConf.set(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_MAX, "1000");
    sparkConf.set(RssClientConfig.RSS_INDEX_READ_LIMIT, "100");
    sparkConf.set(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE, "1m");
    sparkConf.set(RssClientConfig.RSS_HEARTBEAT_INTERVAL, "2000");
  }

  private void verifyTestResult(Map expected, Map actual) {
    assertEquals(expected.size(), actual.size());
    for (Object expectedKey : expected.keySet()) {
      assertEquals(expected.get(expectedKey), actual.get(expectedKey));
    }
  }
}
