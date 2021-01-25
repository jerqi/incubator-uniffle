package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class BufferManager {

  private final ShuffleServer shuffleServer;
  private final ReentrantLock reentrantLock = new ReentrantLock();
  private long capacity;
  private int bufferSize;
  private AtomicLong atomicSize;
  private Map<String, ShuffleBuffer> pool;

  public BufferManager(ShuffleServerConf conf, ShuffleServer shuffleServer) {
    this.capacity = conf.getLong(ShuffleServerConf.BUFFER_CAPACITY);
    this.bufferSize = conf.getInteger(ShuffleServerConf.BUFFER_SIZE);
    this.shuffleServer = shuffleServer;
    this.atomicSize = new AtomicLong(0L);
    this.pool = new ConcurrentHashMap<>();
  }

  public ShuffleBuffer getBuffer(ShuffleEngine shuffleEngine) {
    ShuffleBuffer cur = new ShuffleBuffer(bufferSize, shuffleEngine, this);
    if (isFull()) {
      return null;
    } else {
      // TODO: key already exist
      pool.put(shuffleEngine.makeKey(), cur);
      return cur;
    }
  }

  public void flush() {
    if (reentrantLock.tryLock()) {
      try {
        for (ShuffleBuffer buffer : pool.values()) {
          buffer.flush();
        }
      } finally {
        reentrantLock.unlock();
      }
    }
  }

  public void reclaim(List<String> keys) {
    reentrantLock.lock();
    try {
      for (String key : keys) {
        ShuffleBuffer buffer = pool.remove(key);
        updateSize(buffer.getSize());
        buffer = null;
      }
    } finally {
      reentrantLock.unlock();
    }
  }

  public ShuffleFlushManager getShuffleFlushManager() {
    return shuffleServer.getShuffleFlushManager();
  }

  public long updateSize(long delta) {
    return atomicSize.addAndGet(delta);
  }

  public boolean isFull() {
    return atomicSize.get() >= capacity;
  }

  public void setCapacity(long capacity) {
    this.capacity = capacity;
  }

  public void setBufferSize(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public int getBufferUsedPercent() {
    return (int) (atomicSize.longValue() / (capacity / 100));
  }

  @VisibleForTesting
  Map<String, ShuffleBuffer> getPool() {
    return pool;
  }

  @VisibleForTesting
  AtomicLong getAtomicSize() {
    return atomicSize;
  }
}
