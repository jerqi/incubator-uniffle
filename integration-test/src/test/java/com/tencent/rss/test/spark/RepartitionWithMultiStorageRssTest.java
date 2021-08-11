package com.tencent.rss.test.spark;

import com.google.common.io.Files;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.junit.BeforeClass;

public class RepartitionWithMultiStorageRssTest extends RepartitionTest {
  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();

    // local storage config
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setBoolean(ShuffleServerConf.RSS_USE_MULTI_STORAGE, true);

    // uploader and remote storage config
    shuffleServerConf.setBoolean("rss.server.uploader.enable", true);
    shuffleServerConf.setLong("rss.server.uploader.combine.threshold.MB", 32);
    shuffleServerConf.setLong("rss.server.uploader.references.speed.mbps", 128);
    shuffleServerConf.setString("rss.server.uploader.remote.storage.type", StorageType.HDFS.name());
    shuffleServerConf.setString("rss.server.uploader.base.path", HDFS_URI + "rss/test");
    shuffleServerConf.setLong("rss.server.uploader.interval.ms", 10);
    shuffleServerConf.setInteger("rss.server.uploader.thread.number", 4);

    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  public void updateRssStorage(SparkConf sparkConf) {
    sparkConf.set(RssClientConfig.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
  }
}
