package com.tecent.rss.client;

import java.util.concurrent.atomic.AtomicInteger;

public class ClientUtils {

    private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

    // BlockId is long and composed by executorId and AtomicInteger
    // executorId is high-32 bit and AtomicInteger is low-32 bit
    public static Long getBlockId(long executorId, int atomicInt) {
        if (atomicInt < 0) {
            throw new RuntimeException("Block size is out of scope which is " + Integer.MAX_VALUE);
        }
        return (executorId << 32) + atomicInt;
    }

    public static int getAtomicInteger() {
        return ATOMIC_INT.getAndIncrement();
    }

    public static void main(String[] args) {
        //    println(((Int.MaxValue.toLong) << 32) + (Int.MaxValue))
        //    println((1L << 32) + (Int.MaxValue))
        //    println((0L << 32) + (Int.MaxValue))
        //    val autoInt1 = new AtomicInteger(Int.MaxValue)
        //    println("1:" + autoInt1.getAndIncrement())
        //    println("2:" + autoInt1.getAndIncrement())
        //    println("3:" + autoInt1.getAndIncrement())
    }
}
