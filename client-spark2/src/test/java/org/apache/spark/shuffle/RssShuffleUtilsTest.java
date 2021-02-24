package org.apache.spark.shuffle;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;
import java.util.Random;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.io.CompressionCodec$;
import org.junit.Test;

public class RssShuffleUtilsTest {

  @Test
  public void compressionTest() {
    Set<String> codecNames = Sets.newHashSet("lz4", "snappy", "zstd");
    for (String codecName : codecNames) {
      singleTest(codecName, 1023, 32 * 1024);
      singleTest(codecName, 1024, 32 * 1024);
      singleTest(codecName, 1025, 32 * 1024);
      singleTest(codecName, 1024 * 1024 - 1, 32 * 1024);
      singleTest(codecName, 1024 * 1024, 32 * 1024);
      singleTest(codecName, 1024 * 1024 + 1, 32 * 1024);
      singleTest(codecName, 8 * 1024 * 1024 - 1, 32 * 1024);
      singleTest(codecName, 8 * 1024 * 1024 + 1, 32 * 1024);
    }

    try {
      singleTest("lz4", 1024 * 1024, 64 * 1024);
      fail("Exception should be thrown");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Fail to decompress shuffle data"));
    }
  }

  private void singleTest(String codecName, int size, int compressionBlockSize) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.io.compression.codec", codecName);
    CompressionCodec compressionCodec = CompressionCodec$.MODULE$.createCodec(sparkConf);
    byte[] buf = new byte[size];
    new Random().nextBytes(buf);
    byte[] compressed = RssShuffleUtils.compressData(compressionCodec, buf);
    byte[] uncompressed = RssShuffleUtils.decompressData(compressionCodec, compressed, compressionBlockSize);
    assertArrayEquals(buf, uncompressed);
  }
}
