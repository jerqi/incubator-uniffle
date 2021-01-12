package com.tencent.rss.test;

import static org.junit.Assert.assertEquals;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.coordinator.CoordinatorServer;
import com.tencent.rss.server.ShuffleServer;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.HdfsTestBase;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class IntegrationTestBase extends HdfsTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class);

  private static ShuffleServer shuffleServer;
  private static CoordinatorServer coordinator;

  @BeforeClass
  public static void setupServers() throws Exception {
    // Load configuration from config files
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setString("rss.coordinator.port", "19999");
    coordinatorConf.setString("rss.shuffle.data.replica", "1");
    // Start the coordinator service
    coordinator = new CoordinatorServer(coordinatorConf);
    coordinator.start();

    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setString("rss.server.port", "20001");
    serverConf.setString("rss.storage.type", "FILE");
    serverConf.setString("rss.data.storage.basePath", HDFS_URI + "rss/test");
    serverConf.setString("rss.buffer.capacity", "1000");
    serverConf.setString("rss.buffer.size", "104857600");
    serverConf.setString("rss.coordinator.ip", "127.0.0.1");
    serverConf.setString("rss.coordinator.port", "19999");
    serverConf.setString("rss.heartbeat.delay", "1000");
    serverConf.setString("rss.heartbeat.interval", "2000");
    serverConf.setString("jetty.http.port", "18080");
    serverConf.setString("jetty.corePool.size", "64");

    shuffleServer = new ShuffleServer(serverConf);
    shuffleServer.start();
  }

  @AfterClass
  public static void shutdownServers() throws Exception {
    shuffleServer.stopServer();
    coordinator.stop();
  }

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
    sparkConf.set("spark.rss.partitions.per.server", "2");
    sparkConf.set("spark.rss.writer.buffer.size", "1048576");
    sparkConf.set("spark.rss.writer.buffer.max.size", "2097152");
    sparkConf.set("spark.rss.writer.buffer.spill.size", "10485760");
    sparkConf.set("spark.rss.coordinator.ip", "127.0.0.1");
    sparkConf.set("spark.rss.coordinator.port", "19999");
    sparkConf.set("spark.rss.writer.send.check.timeout", "30000");
    sparkConf.set("spark.rss.writer.send.check.interval", "1000");
    sparkConf.set("spark.rss.base.path", HDFS_URI + "rss/test/");
    sparkConf.set("spark.rss.index.read.limit", "100");
  }

  private void verifyTestResult(Map expected, Map actual) {
    assertEquals(expected.size(), actual.size());
    for (Object expectedKey : expected.keySet()) {
      assertEquals(expected.get(expectedKey), actual.get(expectedKey));
    }
  }
}