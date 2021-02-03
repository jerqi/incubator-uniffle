package com.tencent.rss.test.spark;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.Map;
import java.util.Random;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkSQLTest extends SparkIntegrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSQLTest.class);

  @Test
  public void resultCompareTest() throws Exception {
    run();
  }

  @Override
  public Map runTest(SparkSession spark, String fileName) {
    Dataset<Row> df = spark.read().schema("name STRING, age INT").csv(fileName);
    df.createOrReplaceTempView("people");
    Dataset<Row> queryResult = spark.sql("SELECT name, count(age) FROM people group by name order by name");
    Map<String, Long> result = Maps.newHashMap();
    queryResult.javaRDD().collect().stream().forEach(row -> {
          result.put(row.getString(0), row.getLong(1));
        }
    );
    return result;
  }

  @Override
  public String generateTestFile() throws Exception {
    return generateCsvFile();
  }

  @Override
  public void updateSparkConfCustomer(SparkConf sparkConf) {
    sparkConf.set("spark.sql.shuffle.partitions", "4");
  }

  protected String generateCsvFile() throws Exception {
    int rows = 1000;
    String tempDir = Files.createTempDirectory("rss").toString();
    File file = new File(tempDir, "test.csv");
    file.createNewFile();
    LOG.info("Create file:" + file.getAbsolutePath());
    file.deleteOnExit();
    try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
      for (int i = 0; i < rows; i++) {
        writer.println(generateRecord());
      }
    }
    LOG.info("finish test data for word count file:" + file.getAbsolutePath());
    return file.getAbsolutePath();
  }

  private String generateRecord() {
    Random random = new Random();
    char ch = (char) ('a' + random.nextInt(26));
    int repeats = random.nextInt(10);
    return StringUtils.repeat(ch, repeats) + "," + random.nextInt(100);
  }
}
