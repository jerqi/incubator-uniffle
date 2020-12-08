package com.tencent.rss.common.util;

import static org.junit.Assert.assertEquals;

import java.util.Random;
import java.util.zip.CRC32;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ChecksumUtilsTest {

    @Test
    public void crc32Test() {
        byte[] data = new byte[1024 * 32];
        new Random().nextBytes(data);
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        long expected = crc32.getValue();
        assertEquals(expected, ChecksumUtils.getCrc32(data));
    }
}
