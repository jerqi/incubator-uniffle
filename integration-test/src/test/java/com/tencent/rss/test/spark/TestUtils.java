package com.tencent.rss.test.spark;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class TestUtils {
  private TestUtils() {

  }
  static JavaPairRDD<String, Integer> getRDD(JavaSparkContext jsc) {
    JavaPairRDD<String, Integer> javaPairRDD1 = jsc.parallelizePairs(Lists.newArrayList(
        new Tuple2<>("cat1", 11), new Tuple2<>("dog", 22),
        new Tuple2<>("cat", 33), new Tuple2<>("pig", 44),
        new Tuple2<>("duck", 55), new Tuple2<>("cat", 66)), 2);
    return javaPairRDD1;
  }

  static JavaPairRDD<String, Tuple2<Integer, Integer>> combineByKeyRDD(JavaPairRDD javaPairRDD1) {
    JavaPairRDD<String, Tuple2<Integer, Integer>> javaPairRDD = javaPairRDD1
        .combineByKey((Function<Integer, Tuple2<Integer, Integer>>) i -> new Tuple2<>(1, i),
            (Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>) (tuple, i) ->
                new Tuple2<>(tuple._1 + 1, tuple._2 + i),
            (Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>) (tuple1, tuple2) ->
                new Tuple2<>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)
        );
    return javaPairRDD;
  }
}
