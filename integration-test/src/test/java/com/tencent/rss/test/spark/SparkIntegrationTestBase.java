package com.tencent.rss.test.spark;

import static org.junit.Assert.assertEquals;

import com.tencent.rss.test.IntegrationTestBase;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class SparkIntegrationTestBase extends IntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SparkIntegrationTestBase.class);

  abstract Map runTest(SparkSession spark, String fileName) throws Exception;

  abstract String generateTestFile() throws Exception;

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
    sparkConf.set("spark.rss.writer.buffer.size", "4m");
    sparkConf.set("spark.rss.writer.buffer.spill.size", "32m");
    sparkConf.set("spark.rss.writer.serializer.buffer.size", "128k");
    sparkConf.set("spark.rss.writer.serializer.buffer.max.size", "256k");
    sparkConf.set("spark.rss.coordinator.ip", LOCALHOST);
    sparkConf.set("spark.rss.coordinator.port", "" + COORDINATOR_PORT);
    sparkConf.set("spark.rss.writer.send.check.timeout", "30000");
    sparkConf.set("spark.rss.writer.send.check.interval", "1000");
    sparkConf.set("spark.rss.index.read.limit", "100");
    sparkConf.set("spark.rss.client.read.buffer.size", "1m");
  }

  private void verifyTestResult(Map expected, Map actual) {
    assertEquals(expected.size(), actual.size());
    for (Object expectedKey : expected.keySet()) {
      assertEquals(expected.get(expectedKey), actual.get(expectedKey));
    }
  }
}
