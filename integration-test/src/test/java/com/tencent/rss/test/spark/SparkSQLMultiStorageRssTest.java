package com.tencent.rss.test.spark;

import static org.junit.Assert.assertEquals;

import com.google.common.io.Files;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.junit.BeforeClass;

public class SparkSQLMultiStorageRssTest extends SparkSQLTest {
  private static String basePath;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong("rss.coordinator.app.expired", 5000);
    createCoordinatorServer(coordinatorConf);

    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.heartbeat.interval", 5000);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);

    // local storage config
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);

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
    sparkConf.set(RssClientConfig.RSS_STORAGE_TYPE, "LOCALFILE");
  }

  @Override
  public void checkShuffleData() throws Exception {
    Thread.sleep(12000);
    String[] paths = basePath.split(",");
    for (String path : paths) {
      File f = new File(path);
      assertEquals(0, f.list().length);
    }
  }
}
