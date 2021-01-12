package com.tencent.rss.test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class RepartitionShuffleTest extends IntegrationTestBase implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class);

  @Test
  public void resultCompareTest() throws Exception {
    run();
  }

  @Test
  public void testMemoryRelease() throws Exception {
    String fileName = generateTextFile(10000, 10000);
    SparkConf sparkConf = createSparkConf();
    updateSparkConfWithRss(sparkConf);
    sparkConf.set("spark.executor.memory", "500m");
    sparkConf.set("spark.rss.writer.buffer.size", "4m");
    sparkConf.set("spark.rss.writer.buffer.max.size", "8m");
    sparkConf.set("spark.rss.writer.buffer.spill.size", "20m");

    // oom if there has no memory release
    runSparkApp(sparkConf, fileName);
  }

  @Override
  public Map runTest(SparkSession spark, String fileName) {
    return repartitionApp(spark, fileName);
  }

  @Override
  public String generateTestFile() throws Exception {
    return generateTextFile(1000, 5000);
  }

  private String generateTextFile(int wordsPerRow, int rows) throws Exception {
    String tempDir = Files.createTempDirectory("rss").toString();
    File file = new File(tempDir, "wordcount.txt");
    file.createNewFile();
    LOG.info("Create file:" + file.getAbsolutePath());
    file.deleteOnExit();
    try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
      for (int i = 0; i < rows; i++) {
        writer.println(getLine(wordsPerRow));
      }
    }
    LOG.info("finish test data for word count file:" + file.getAbsolutePath());
    return file.getAbsolutePath();
  }

  private String generateString(int length) {
    Random random = new Random();
    char ch = (char) ('a' + random.nextInt(26));
    int repeats = random.nextInt(length);
    return StringUtils.repeat(ch, repeats);
  }

  private String getLine(int wordsPerRow) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < wordsPerRow; i++) {
      sb.append(generateString(10));
      sb.append(" ");
    }
    return sb.toString();
  }

  private Map repartitionApp(SparkSession spark, String fileName) {
    JavaRDD<String> lines = spark.read().textFile(fileName).javaRDD();
    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1)).repartition(5);
    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
    return counts.sortByKey().collectAsMap();
  }
}
