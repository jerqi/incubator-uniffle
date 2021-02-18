package com.tencent.rss.server;

import com.google.common.collect.Sets;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ShuffleBuffer {

  private final int capacity;
  private Set<Long> eventIds = Sets.newConcurrentHashSet();

  private int size;
  private List<ShufflePartitionedBlock> blocks;

  public ShuffleBuffer(int capacity) {
    this.capacity = capacity;
    this.size = 0;
    this.blocks = new LinkedList<>();
  }

  public int append(ShufflePartitionedData data) {
    int mSize = 0;

    synchronized (this) {
      for (ShufflePartitionedBlock block : data.getBlockList()) {
        blocks.add(block);
        mSize += block.getLength();
        size += mSize;
      }
      ShuffleServerMetrics.gaugeBufferDataSize.inc(size);
    }

    return mSize;
  }

  public synchronized ShuffleDataFlushEvent toFlushEvent(
      String appId, int shuffleId, int startPartition, int endPartition) {
    if (blocks.isEmpty()) {
      return null;
    }
    // buffer will be cleared, and new list must be created for async flush
    List<ShufflePartitionedBlock> spBlocks = new LinkedList<>(blocks);
    ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(
        ShuffleFlushManager.ATOMIC_EVENT_ID.getAndIncrement(),
        appId,
        shuffleId,
        startPartition,
        endPartition,
        size,
        spBlocks);
    eventIds.add(event.getEventId());
    blocks.clear();
    size = 0;
    return event;
  }

  public synchronized Set<Long> getAndClearEventIds() {
    Set<Long> snapshot = Sets.newHashSet(eventIds);
    eventIds.clear();
    return snapshot;
  }

  public List<ShufflePartitionedBlock> getBlocks() {
    return blocks;
  }

  public int getSize() {
    return size;
  }

  public boolean isFull() {
    return size > capacity;
  }

}
