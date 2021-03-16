package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleBufferManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferManager.class);

  private final ShuffleFlushManager shuffleFlushManager;
  private long capacity;
  private long spillThreshold;
  private int bufferSize;
  private int retryNum;
  private AtomicLong preAllocatedSize = new AtomicLong(0L);
  private AtomicLong inFlushSize = new AtomicLong(0L);
  private AtomicLong atomicSize = new AtomicLong(0L);
  // appId -> shuffleId -> partitionId -> ShuffleBuffer to avoid too many appId
  private Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool;


  public ShuffleBufferManager(ShuffleServerConf conf, ShuffleFlushManager shuffleFlushManager) {
    this.capacity = conf.getLong(ShuffleServerConf.BUFFER_CAPACITY);
    this.bufferSize = conf.getInteger(ShuffleServerConf.BUFFER_SIZE);
    this.spillThreshold = conf.getLong(ShuffleServerConf.BUFFER_SPILL_THRESHOLD);
    this.shuffleFlushManager = shuffleFlushManager;
    this.bufferPool = new ConcurrentHashMap<>();
    this.retryNum = conf.getInteger(ShuffleServerConf.SERVER_MEMORY_REQUEST_RETRY_MAX);
  }

  public StatusCode registerBuffer(String appId, int shuffleId, int startPartition, int endPartition) {
    bufferPool.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    shuffleIdToBuffers.putIfAbsent(shuffleId, TreeRangeMap.create());
    RangeMap<Integer, ShuffleBuffer> bufferRangeMap = shuffleIdToBuffers.get(shuffleId);
    if (bufferRangeMap.get(startPartition) == null) {
      bufferRangeMap.put(Range.closed(startPartition, endPartition), new ShuffleBuffer(bufferSize));
    } else {
      LOG.warn("Already register for appId[" + appId + "], shuffleId[" + shuffleId + "], startPartition["
          + startPartition + "], endPartition[" + endPartition + "]");
    }

    return StatusCode.SUCCESS;
  }

  public StatusCode cacheShuffleData(String appId, int shuffleId,
      boolean isPreAllocated, ShufflePartitionedData spd) {
    if (!isPreAllocated && isFull()) {
      LOG.warn("Got unexpect data, can't cache it because the space is full");
      return StatusCode.NO_BUFFER;
    }
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    if (shuffleIdToBuffers == null) {
      return StatusCode.NO_REGISTER;
    }
    RangeMap<Integer, ShuffleBuffer> rangeToBuffers = shuffleIdToBuffers.get(shuffleId);
    if (rangeToBuffers == null) {
      return StatusCode.NO_REGISTER;
    }
    Entry<Range<Integer>, ShuffleBuffer> entry = rangeToBuffers.getEntry(spd.getPartitionId());
    if (entry == null) {
      return StatusCode.NO_REGISTER;
    }
    ShuffleBuffer buffer = entry.getValue();
    Range<Integer> range = entry.getKey();
    int size = buffer.append(spd);
    updateSize(size, isPreAllocated);
    if (shouldFlush()) {
      flush();
    } else if (buffer.isFull()) {
      flushBuffer(buffer, appId, shuffleId, range.lowerEndpoint(), range.upperEndpoint());
    }
    return StatusCode.SUCCESS;
  }

  public synchronized void commitShuffleTask(String appId, int shuffleId) {
    RangeMap<Integer, ShuffleBuffer> buffers = bufferPool.get(appId).get(shuffleId);
    for (Map.Entry<Range<Integer>, ShuffleBuffer> entry : buffers.asMapOfRanges().entrySet()) {
      ShuffleBuffer buffer = entry.getValue();
      Range<Integer> range = entry.getKey();
      flushBuffer(buffer, appId, shuffleId, range.lowerEndpoint(), range.upperEndpoint());
    }
  }

  // buffer pool is full, flush all buffers
  public synchronized void flush() {
    for (Entry<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> appIdToBuffers : bufferPool.entrySet()) {
      String appId = appIdToBuffers.getKey();
      for (Entry<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers :
          appIdToBuffers.getValue().entrySet()) {
        int shuffleId = shuffleIdToBuffers.getKey();
        for (Entry<Range<Integer>, ShuffleBuffer> rangeEntry :
            shuffleIdToBuffers.getValue().asMapOfRanges().entrySet()) {
          Range<Integer> range = rangeEntry.getKey();
          flushBuffer(rangeEntry.getValue(), appId, shuffleId,
              range.lowerEndpoint(), range.upperEndpoint());
        }
      }
    }
  }

  private synchronized void flushBuffer(ShuffleBuffer buffer, String appId,
      int shuffleId, int startPartition, int endPartition) {
    ShuffleDataFlushEvent event =
        buffer.toFlushEvent(appId, shuffleId, startPartition, endPartition);
    if (event != null) {
      inFlushSize.addAndGet(event.getSize());
      shuffleFlushManager.addToFlushQueue(event);
    }
  }

  public void removeBuffer(String appId) {
    Map<Integer, RangeMap<Integer, ShuffleBuffer>> shuffleIdToBuffers = bufferPool.get(appId);
    if (shuffleIdToBuffers == null) {
      return;
    }
    // calculate released size
    long size = 0;
    for (RangeMap<Integer, ShuffleBuffer> rangeMap : shuffleIdToBuffers.values()) {
      if (rangeMap != null) {
        Collection<ShuffleBuffer> buffers = rangeMap.asMapOfRanges().values();
        if (buffers != null) {
          for (ShuffleBuffer buffer : buffers) {
            size += buffer.getSize();
          }
        }
      }
    }
    // release memory
    releaseMemory(size);
    bufferPool.remove(appId);
  }

  public synchronized boolean requireMemory(long size, boolean isPreAllocated) {
    if (capacity - atomicSize.get() >= size) {
      atomicSize.addAndGet(size);
      if (isPreAllocated) {
        requirePreAllocatedSize(size);
      }
      return true;
    }
    LOG.warn("Require memory failed with " + size + " bytes, total[" + atomicSize.get()
        + "] include preAllocation[" + preAllocatedSize.get()
        + "], inFlushSize[" + inFlushSize.get() + "]");
    return false;
  }

  public synchronized void releaseMemory(long size) {
    if (atomicSize.get() >= size) {
      atomicSize.addAndGet(-size);
    } else {
      LOG.warn("Current allocated memory[" + atomicSize.get()
          + "] is less than released[" + size + "], set allocated memory to 0");
      atomicSize.set(0L);
    }
  }

  public synchronized void releaseFlushMemory(long size) {
    if (inFlushSize.get() >= size) {
      inFlushSize.addAndGet(-size);
    } else {
      LOG.warn("Current in flush memory[" + inFlushSize.get()
          + "] is less than released[" + size + "], set allocated memory to 0");
      inFlushSize.set(0L);
    }
  }

  public boolean requireMemoryWithRetry(long size) {
    boolean result = false;
    for (int i = 0; i < retryNum; i++) {
      result = requireMemory(size, false);
      if (result) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
        LOG.warn("Error happened when require memory", e);
      }
    }
    return result;
  }

  void updateSize(long delta, boolean isPreAllocated) {
    if (isPreAllocated) {
      releasePreAllocatedSize(delta);
    } else {
      // add size if not allocated
      atomicSize.addAndGet(delta);
    }
  }

  void requirePreAllocatedSize(long delta) {
    preAllocatedSize.addAndGet(delta);
  }

  void releasePreAllocatedSize(long delta) {
    preAllocatedSize.addAndGet(-delta);
  }

  // if data size in buffer > spillThreshold, do the flush
  boolean shouldFlush() {
    return atomicSize.get() - preAllocatedSize.get() - inFlushSize.get() > spillThreshold;
  }

  boolean isFull() {
    return atomicSize.get() >= capacity;
  }

  @VisibleForTesting
  protected Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> getBufferPool() {
    return bufferPool;
  }

  int getBufferUsedPercent() {
    return (int) (atomicSize.longValue() / (capacity / 100));
  }

  @VisibleForTesting
  public long getSize() {
    return atomicSize.get();
  }

  @VisibleForTesting
  public long getInFlushSize() {
    return inFlushSize.get();
  }

  @VisibleForTesting
  void resetSize() {
    atomicSize = new AtomicLong(0L);
    preAllocatedSize = new AtomicLong(0L);
    inFlushSize = new AtomicLong(0L);
  }

  @VisibleForTesting
  public long getPreAllocatedSize() {
    return preAllocatedSize.get();
  }
}
