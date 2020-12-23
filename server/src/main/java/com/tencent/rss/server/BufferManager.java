package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicInteger;

public class BufferManager {
  private int capacity;
  private int bufferSize;
  private int bufferTTL;
  private AtomicInteger atomicCount;

  public BufferManager(ShuffleServerConf conf) {
    this.capacity = conf.getInteger(ShuffleServerConf.BUFFER_CAPACITY);
    this.bufferSize = conf.getInteger(ShuffleServerConf.BUFFER_SIZE);
    this.atomicCount = new AtomicInteger(0);
  }

  public BufferManager(int capacity, int bufferSize, int bufferTTL) {
    this.capacity = capacity;
    this.bufferSize = bufferSize;
    this.bufferTTL = bufferTTL;
    this.atomicCount = new AtomicInteger(0);
  }

  public ShuffleBuffer getBuffer(int start, int end) {
    if (!getBufferQuota()) {
      return null;
    }

    return new ShuffleBuffer(bufferSize, bufferTTL, start, end);
  }

  public boolean isFull() {
    return atomicCount.get() >= capacity;
  }

  private boolean getBufferQuota() {
    int cur = atomicCount.get();
    if (cur > capacity) {
      return false;
    }

    cur = atomicCount.addAndGet(1);
    if (cur > capacity) {
      atomicCount.decrementAndGet();
      return false;
    } else {
      return true;
    }
  }

  @VisibleForTesting
  AtomicInteger getAtomicCount() {
    return atomicCount;
  }

  public int getAvailableCount() {
    return capacity - atomicCount.get();
  }

}
