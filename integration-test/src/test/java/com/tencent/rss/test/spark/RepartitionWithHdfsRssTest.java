package com.tencent.rss.test.spark;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
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
    sparkConf.set(RssClientConfig.RSS_STORAGE_TYPE, "HDFS");
    sparkConf.set(RssClientConfig.RSS_BASE_PATH, HDFS_URI + "rss/test");
  }
}
