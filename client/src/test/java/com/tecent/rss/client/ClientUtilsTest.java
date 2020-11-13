package com.tecent.rss.client;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ClientUtilsTest {

    @Test
    public void getBlockIdTest() {
        assertTrue(
                9223372034707292159L == ClientUtils.getBlockId(Integer.MAX_VALUE, Integer.MAX_VALUE));
        assertTrue(0L == ClientUtils.getBlockId(0, 0));
        assertTrue(2147483647L == ClientUtils.getBlockId(0, Integer.MAX_VALUE));
        assertTrue(6442450943L == ClientUtils.getBlockId(1, Integer.MAX_VALUE));
        assertTrue(9223372032559808513L == ClientUtils.getBlockId(Integer.MAX_VALUE, 1));
        assertTrue(5299989644498L == ClientUtils.getBlockId(1234, 1234));
    }

    @Test
    public void getAtomicIntegerTest() {
        int atomicId = ClientUtils.getAtomicInteger();
        assertTrue((atomicId + 1) == ClientUtils.getAtomicInteger());
    }
}
