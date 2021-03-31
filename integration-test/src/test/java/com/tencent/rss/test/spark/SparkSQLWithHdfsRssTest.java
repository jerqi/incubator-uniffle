package com.tencent.rss.test.spark;

import static org.junit.Assert.assertEquals;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.junit.BeforeClass;

public class SparkSQLWithHdfsRssTest extends SparkSQLTest {

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 1000);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
    sparkConf.set("spark.rss.storage.type", "HDFS");
    sparkConf.set("spark.rss.base.path", HDFS_URI + "rss/test");
  }

  @Override
  public void checkShuffleData() throws Exception {
    Thread.sleep(12000);
    String path = HDFS_URI + "rss/test";
    assertEquals(0, fs.listStatus(new Path(path)).length);
  }
}
