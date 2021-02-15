package com.tencent.rss.test.spark;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import org.apache.spark.SparkConf;
import org.junit.BeforeClass;

public class RepartitionWithHdfsRssTest extends RepartitionTest {

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
    sparkConf.set("spark.rss.storage.type", "HDFS");
    sparkConf.set("spark.rss.base.path", HDFS_URI + "rss/test");
  }
}
