package com.tencent.rss.test.spark;

import com.google.common.collect.Lists;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

public class GroupByKeyTest extends SimpleTestBase {

  @Test
  public void groupByTest() throws Exception {
    run();
  }

  @Override
  public Map runTest(SparkSession spark, String fileName) throws Exception {
    // take a rest to make sure shuffle server is registered
    Thread.sleep(3000);
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    JavaPairRDD<String, String> javaPairRDD1 = jsc.parallelizePairs(Lists.newArrayList(
        new Tuple2<>("a", "1"), new Tuple2<>("b", "2"),
        new Tuple2<>("c", "3"), new Tuple2<>("d", "4")), 2);
    JavaPairRDD<String, Iterable<String>> javaPairRDD = javaPairRDD1.groupByKey().sortByKey();
    return javaPairRDD.collectAsMap();
  }
}
