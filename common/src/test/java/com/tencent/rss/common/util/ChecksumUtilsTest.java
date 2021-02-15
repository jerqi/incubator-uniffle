package com.tencent.rss.common.util;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.zip.CRC32;
import org.junit.Test;

public class ChecksumUtilsTest {

  @Test
  public void crc32Test() {
    byte[] data = new byte[32 * 1024 * 1024];
    new Random().nextBytes(data);
    CRC32 crc32 = new CRC32();
    crc32.update(data);
    long expected = crc32.getValue();
    assertEquals(expected, ChecksumUtils.getCrc32(data));

    data = new byte[32 * 1024];
    new Random().nextBytes(data);
    crc32 = new CRC32();
    crc32.update(data);
    expected = crc32.getValue();
    assertEquals(expected, ChecksumUtils.getCrc32(data));
  }
}
