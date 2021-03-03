package com.tencent.rss.common.util;

import java.util.zip.CRC32;

public class ChecksumUtils {

  private static final int LENGTH_PER_CRC = 4 * 1024;

  public static long getCrc32(byte[] buf) {
    CRC32 crc32 = new CRC32();

    for (int i = 0; i < buf.length; ) {
      int len = Math.min(LENGTH_PER_CRC, buf.length - i);
      crc32.update(buf, i, len);
      i += len;
    }

    return crc32.getValue();
  }
}
