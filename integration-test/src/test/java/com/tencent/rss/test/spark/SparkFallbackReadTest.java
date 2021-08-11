package com.tencent.rss.test.spark;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class SparkFallbackReadTest extends SparkIntegrationTestBase {

  private static File tmpDir;
  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    tmpDir.deleteOnExit();
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString(ShuffleServerConf.RSS_HDFS_BASE_PATH,  HDFS_URI + "rss/multi_storage_integration");
    shuffleServerConf.setDouble(ShuffleServerConf.RSS_CLEANUP_THRESHOLD, 0.0);
    shuffleServerConf.setDouble(ShuffleServerConf.RSS_HIGH_WATER_MARK_OF_WRITE, 100.0);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_DISK_CAPACITY, 1024L * 1024L * 100);
    shuffleServerConf.setBoolean(ShuffleServerConf.RSS_UPLOADER_ENABLE, true);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_PENDING_EVENT_TIMEOUT_SEC, 30L);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_UPLOAD_COMBINE_THRESHOLD_MB, 1L);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_SHUFFLE_EXPIRED_TIMEOUT_MS, 5000L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 5L * 1000L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 20L * 1000L);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_PENDING_EVENT_TIMEOUT_SEC, 15);
    shuffleServerConf.setBoolean(ShuffleServerConf.RSS_USE_MULTI_STORAGE, true);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Test
  public void resultCompareTest() throws Exception {
    run();
    checkShuffleData();
  }

  @Override
  Map runTest(SparkSession spark, String fileName) throws Exception {
    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    JavaPairRDD<String, Integer> javaPairRDD1 = jsc.parallelizePairs(Lists.newArrayList(
        new Tuple2<>("cat", 11), new Tuple2<>("dog", 22),
        new Tuple2<>("cat", 33), new Tuple2<>("pig", 44),
        new Tuple2<>("duck", 55), new Tuple2<>("cat", 66)), 2);
    JavaPairRDD<String, Tuple2<Integer, Integer>> javaPairRDD = javaPairRDD1
        .combineByKey((Function<Integer, Tuple2<Integer, Integer>>) i -> new Tuple2<>(1, i),
            (Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>) (tuple, i) ->
                new Tuple2<>(tuple._1 + 1, tuple._2 + i),
            (Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>) (tuple1, tuple2) ->
                new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
        );
    final long  ts = System.currentTimeMillis();
    javaPairRDD.foreach(partition -> {
      long local = System.currentTimeMillis();
      if (partition._1.equals("duck") && local - ts < 10000) {
        Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
        throw new RuntimeException("oops", new IllegalArgumentException("test exception"));
      }
    });
    return javaPairRDD.collectAsMap();
  }

  @Override
  public void updateCommonSparkConf(SparkConf sparkConf) {
    sparkConf.setMaster("local[4, 2]");
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.setMaster("local[4, 2]");
    sparkConf.set(RssClientConfig.RSS_STORAGE_TYPE, "LOCALFILE");
    sparkConf.set(RssClientConfig.RSS_BASE_PATH, HDFS_URI + "rss/multi_storage_integration");
  }

  private void checkShuffleData() {
    try {
      String hdfsPath = HDFS_URI + "rss/multi_storage_integration";
      File localPath1 = new File(tmpDir, "data1");
      File localPath2 = new File(tmpDir, "data2");
      File[] files1 = localPath1.listFiles();
      File[] files2 = localPath2.listFiles();
      if (files1 != null) {
        for (File file : files1) {
          File[] shuffles = file.listFiles();
          assertEquals(0, shuffles.length);
        }
      }

      if (files2 != null) {
        for (File file : files2) {
          File[] shuffles = file.listFiles();
          assertEquals(0, shuffles.length);
        }
      }
      FileStatus[] files = fs.listStatus(new Path(hdfsPath));
      if (files != null) {
        for (FileStatus file : files) {
          FileStatus[] shuffles = fs.listStatus(file.getPath());
          for (FileStatus another : shuffles) {
            assertEquals(1, shuffles.length);
          }
          Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
          boolean isException = false;
          try {
            fs.listStatus(file.getPath());
          } catch (FileNotFoundException fe) {
            isException = true;
            assertTrue(fe.getMessage().contains("multi_storage_integration"));
          }
          assertTrue(isException);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
