package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;

import java.util.LinkedList;
import java.util.List;

public class ShuffleBuffer {

  private final int capacity;
  private final BufferManager bufferManager;
  private final ShuffleEngine shuffleEngine;

  private int size;
  private List<ShufflePartitionedBlock> blocks;

  public ShuffleBuffer(
      int capacity,
      ShuffleEngine shuffleEngine,
      BufferManager bufferManager) {
    this.capacity = capacity;
    this.shuffleEngine = shuffleEngine;
    this.bufferManager = bufferManager;
    this.size = 0;
    this.blocks = new LinkedList<>();
  }

  public void clear() {
    blocks.clear();
    size = 0;
  }

  public StatusCode append(ShufflePartitionedData data) {
      int mSize = 0;

      synchronized (this) {
        for (ShufflePartitionedBlock block : data.getBlockList()) {
          blocks.add(block);
          mSize += block.getLength();
          size += mSize;
        }
      }

      bufferManager.updateSize(mSize);
      ShuffleServerMetrics.incBufferedBlockSize(mSize);


      if (bufferManager.isFull()) {
        bufferManager.flush();
      } else if (isFull()) {
        flush();
      }

    return StatusCode.SUCCESS;
  }

  public ShuffleDataFlushEvent flush() {
    if (size == 0) {
      return null;
    }

    synchronized (this) {
      if (size == 0) {
        return null;
      } else {
        return asyncFlush();

      }
    }
  }


  // add blocks to queue, they will be flushed to storage asynchronous
  private ShuffleDataFlushEvent asyncFlush() throws IllegalStateException {
    // buffer will be cleared, and new list must be created for async flush
    ShuffleFlushManager shuffleFlushManager = bufferManager.getShuffleFlushManager();
    List<ShufflePartitionedBlock> spBlocks = new LinkedList<>(getBlocks());
    ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(
        ShuffleFlushManager.ATOMIC_EVENT_ID.getAndIncrement(),
        shuffleEngine.getAppId(),
        shuffleEngine.getShuffleId(),
        shuffleEngine.getStartPartition(),
        shuffleEngine.getEndPartition(),
        size,
        spBlocks);
    shuffleFlushManager.addToFlushQueue(event);
    shuffleEngine.getEventIds().add(event.getEventId());
    clear();
    return event;
  }

  public List<ShufflePartitionedBlock> getBlocks() {
    return blocks;
  }

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public boolean isFull() {
    return size > capacity;
  }

  @VisibleForTesting
  int getCapacity() {
    return this.capacity;
  }
}
