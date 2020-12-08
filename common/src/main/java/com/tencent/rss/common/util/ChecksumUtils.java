package com.tencent.rss.common.util;

import java.util.zip.CRC32;

public class ChecksumUtils {

    public static long getCrc32(byte[] buf) {
        CRC32 crc32 = new CRC32();
        crc32.update(buf);
        return crc32.getValue();
    }
}
