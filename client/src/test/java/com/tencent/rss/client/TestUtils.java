package com.tencent.rss.client;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUtils {

  private TestUtils() {

  }

  public static void validateResult(ShuffleReadClient readClient,
                                Map<Long, byte[]> expectedData) {
    ByteBuffer data = readClient.readShuffleBlockData().getByteBuffer();
    int blockNum = 0;
    while (data != null) {
      blockNum++;
      boolean match = false;
      for (byte[] expected : expectedData.values()) {
        if (compareByte(expected, data)) {
          match = true;
        }
      }
      assertTrue(match);
      CompressedShuffleBlock csb = readClient.readShuffleBlockData();
      if (csb == null) {
        data = null;
      } else {
        data = csb.getByteBuffer();
      }
    }
    assertEquals(expectedData.size(), blockNum);
  }

  public static boolean compareByte(byte[] expected, ByteBuffer buffer) {
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != buffer.get(i)) {
        return false;
      }
    }
    return true;
  }
}
