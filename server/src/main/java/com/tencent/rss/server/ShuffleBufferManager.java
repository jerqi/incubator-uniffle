package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleBufferManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferManager.class);

  private final ShuffleFlushManager shuffleFlushManager;
  private long capacity;
  private int bufferSize;
  private AtomicLong atomicSize;
  // appId -> shuffleId -> partitionId -> ShuffleBuffer to avoid too many appId
  private Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool;

  public ShuffleBufferManager(ShuffleServerConf conf, ShuffleFlushManager shuffleFlushManager) {
    this.capacity = conf.getLong(ShuffleServerConf.BUFFER_CAPACITY);
    this.bufferSize = conf.getInteger(ShuffleServerConf.BUFFER_SIZE);
    this.shuffleFlushManager = shuffleFlushManager;
    this.atomicSize = new AtomicLong(0L);
    this.bufferPool = new ConcurrentHashMap<>();
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

  public synchronized StatusCode cacheShuffleData(String appId, int shuffleId, ShufflePartitionedData spd) {
    if (isFull()) {
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
    // add size
    updateSize(size);
    if (isFull()) {
      flush();
    } else if (buffer.isFull()) {
      flushBufferToFlushQueue(buffer, appId, shuffleId, range.lowerEndpoint(), range.upperEndpoint());
    }
    return StatusCode.SUCCESS;
  }

  public synchronized RangeMap<Integer, Set<Long>> commitShuffleTask(String appId, int shuffleId) {
    RangeMap<Integer, Set<Long>> partitionToEventIds = TreeRangeMap.create();
    RangeMap<Integer, ShuffleBuffer> buffers = bufferPool.get(appId).get(shuffleId);
    for (Map.Entry<Range<Integer>, ShuffleBuffer> entry : buffers.asMapOfRanges().entrySet()) {
      ShuffleBuffer buffer = entry.getValue();
      Range<Integer> range = entry.getKey();
      flushBufferToFlushQueue(buffer, appId, shuffleId, range.lowerEndpoint(), range.upperEndpoint());
      Set<Long> snapshot = buffer.getAndClearEventIds();
      // skip empty eventId
      if (!snapshot.isEmpty()) {
        partitionToEventIds.put(range, snapshot);
        LOG.debug("Commit for appId[" + appId + "], shuffleId[" + shuffleId + "], partitionRange["
            + range + "] and get expectedEventIds: " + snapshot);
      }
    }
    return partitionToEventIds;
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
          flushBufferToFlushQueue(rangeEntry.getValue(), appId, shuffleId,
              range.lowerEndpoint(), range.upperEndpoint());
        }
      }
    }
  }

  private synchronized void flushBufferToFlushQueue(ShuffleBuffer buffer, String appId,
      int shuffleId, int startPartition, int endPartition) {
    ShuffleDataFlushEvent event =
        buffer.toFlushEvent(appId, shuffleId, startPartition, endPartition);
    if (event != null) {
      shuffleFlushManager.addToFlushQueue(event);
    }
  }

  long updateSize(long delta) {
    return atomicSize.addAndGet(delta);
  }

  boolean isFull() {
    return atomicSize.get() > capacity;
  }

  @VisibleForTesting
  protected Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> getBufferPool() {
    return bufferPool;
  }

  int getBufferUsedPercent() {
    return (int) (atomicSize.longValue() / (capacity / 100));
  }

  @VisibleForTesting
  long getSize() {
    return atomicSize.get();
  }

  @VisibleForTesting
  void resetSize() {
    atomicSize = new AtomicLong(0L);
  }
}
