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
import org.junit.After;
import org.junit.Before;

abstract public class IntegrationTestBase extends HdfsTestBase {

  private ShuffleServer shuffleServer;
  private CoordinatorServer coordinator;

  @Before
  public void setupServers() throws Exception {
    // Load configuration from config files
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setString("com.tencent.rss.coordinator.port", "19999");
    coordinatorConf.setString("com.tencent.rss.shuffle.data.replica", "1");
    // Start the coordinator service
    coordinator = new CoordinatorServer(coordinatorConf);
    coordinator.start();

    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setString("rss.server.port", "20001");
    serverConf.setString("rss.storage.type", "FILE");
    serverConf.setString("rss.data.storage.basePath", HDFS_URI + "rss/test");
    serverConf.setString("rss.buffer.capacity", "1000");
    serverConf.setString("rss.buffer.size", "10485760");
    serverConf.setString("rss.coordinator.ip", "127.0.0.1");
    serverConf.setString("rss.coordinator.port", "19999");
    serverConf.setString("rss.heartbeat.delay", "1000");
    serverConf.setString("rss.heartbeat.interval", "2000");
    serverConf.setString("jetty.http.port", "18080");
    serverConf.setString("jetty.corePool.size", "64");

    shuffleServer = new ShuffleServer(serverConf);
    shuffleServer.start();
  }

  @After
  public void shutdownServers() throws Exception {
    shuffleServer.stopServer();
    coordinator.stop();
  }

  abstract Map runTest(SparkSession spark, String fileName) throws Exception;

  abstract String generateTestFile() throws Exception;

  public void updateSparkConfCustomer(SparkConf sparkConf) {
  }

  public void run() throws Exception {

    String fileName = generateTestFile();

    SparkConf sparkConf = new SparkConf()
        .setAppName(this.getClass().getSimpleName())
        .setMaster("local[4]");
    SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

    Map resultWithoutRss = runTest(spark, fileName);
    spark.stop();

    updateSparkConfWithRss(sparkConf);
    updateSparkConfCustomer(sparkConf);
    spark = SparkSession.builder().config(sparkConf).getOrCreate();

    Map resultWithRss = runTest(spark, fileName);

    verifyTestResult(resultWithoutRss, resultWithRss);

    // Stop SparkContext before the job is done
    spark.stop();
  }

  public void updateSparkConfWithRss(SparkConf sparkConf) {
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager");
    sparkConf.set("spark.rss.partitions.per.server", "2");
    sparkConf.set("spark.rss.writer.buffer.size", "1048576");
    sparkConf.set("spark.rss.writer.buffer.max.size", "2097152");
    sparkConf.set("spark.rss.writer.buffer.spill.size", "10485760");
    sparkConf.set("spark.rss.coordinator.ip", "127.0.0.1");
    sparkConf.set("spark.rss.coordinator.port", "19999");
    sparkConf.set("spark.rss.writer.send.check.timeout", "10000");
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