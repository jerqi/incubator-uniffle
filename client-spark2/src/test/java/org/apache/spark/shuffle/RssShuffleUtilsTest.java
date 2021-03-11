package org.apache.spark.shuffle;

import static org.junit.Assert.assertArrayEquals;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Random;
import org.junit.Test;

public class RssShuffleUtilsTest {

  @Test
  public void compressionTest() {
    List<Integer> testSizes = Lists.newArrayList(
        1, 1024, 128 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024);
    for (int size : testSizes) {
      singleTest(size);
    }
  }

  private void singleTest(int size) {
    byte[] buf = new byte[size];
    new Random().nextBytes(buf);

    byte[] compressed = RssShuffleUtils.compressData(buf);
    byte[] uncompressed = RssShuffleUtils.decompressData(compressed, size);
    assertArrayEquals(buf, uncompressed);
  }
}
