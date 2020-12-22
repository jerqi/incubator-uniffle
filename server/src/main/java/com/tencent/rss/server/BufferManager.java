package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicInteger;

public class BufferManager {
  private static final BufferManager INSTANCE = new BufferManager();

  private int capacity;
  private int bufferSize;
  private int bufferTTL;
  private AtomicInteger atomicCount;

  private BufferManager() {
  }

  public static BufferManager instance() {
    return INSTANCE;
  }

  public boolean init(ShuffleServerConf conf) {
    return init(
      conf.getInteger(ShuffleServerConf.BUFFER_CAPACITY),
      conf.getInteger(ShuffleServerConf.BUFFER_SIZE),
      0);
  }

  public boolean init(int capacity, int bufferSize, int bufferTTL) {
    this.capacity = capacity;
    this.bufferSize = bufferSize;
    this.bufferTTL = bufferTTL;
    this.atomicCount = new AtomicInteger(0);
    return true;
  }

  public ShuffleBuffer getBuffer(int start, int end) {
    if (!getBufferQuota()) {
      return null;
    }

    return new ShuffleBuffer(bufferSize, bufferTTL, start, end);
  }

  private boolean getBufferQuota() {
    int cur = atomicCount.get();
    if (cur > capacity) {
      return false;
    }

    cur = atomicCount.addAndGet(1);
    if (cur > capacity) {
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
