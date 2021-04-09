package com.tencent.rss.test.spark;

import com.google.common.collect.Lists;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import java.util.Map;
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

public class CombineByKeyTest extends SparkIntegrationTestBase {

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set(RssClientConfig.RSS_STORAGE_TYPE, "HDFS");
    sparkConf.set(RssClientConfig.RSS_BASE_PATH, HDFS_URI + "rss/test");
  }

  @Test
  public void resultCompareTest() throws Exception {
    run();
  }

  @Override
  public Map runTest(SparkSession spark, String fileName) throws Exception {
    // take a rest to make sure shuffle server is registered
    Thread.sleep(3000);
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
    return javaPairRDD.collectAsMap();
  }
}
