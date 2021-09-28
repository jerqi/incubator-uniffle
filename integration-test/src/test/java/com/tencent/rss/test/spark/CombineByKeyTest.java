package com.tencent.rss.test.spark;

import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import scala.Tuple2;

public class CombineByKeyTest extends SimpleTestBase {

  @Test
  public void combineByKeyTest() throws Exception {
    run();
  }

  @Override
  public Map runTest(SparkSession spark, String fileName) throws Exception {
    // take a rest to make sure shuffle server is registered
    Thread.sleep(4000);
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    JavaPairRDD<String, Tuple2<Integer, Integer>> javaPairRDD = TestUtils.combineByKeyRDD(
        TestUtils.getRDD(jsc));
    return javaPairRDD.collectAsMap();
  }
}
