package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.proto.RssProtos.StatusCode;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ShuffleBuffer {

  private final int capacity;
  private final int ttl;
  private final int start; // start partition
  private final int end; // end partition
  private int size;
  private int blockNum;
  private Map<Integer, List<ShufflePartitionedBlock>> partitionBuffers;

  public ShuffleBuffer(int capacity, int ttl, int start, int end) {
    this.capacity = capacity;
    this.ttl = ttl;
    this.start = start;
    this.end = end;

    partitionBuffers = new HashMap<>();
    for (int i = start; i <= end; ++i) {
      partitionBuffers.put(i, new LinkedList<>());
    }
  }

  public void clear() {
    if (partitionBuffers != null) {
      partitionBuffers.forEach((k, v) -> v.clear());
      ShuffleServerMetrics.decBufferedBlockSize(size);
      ShuffleServerMetrics.decBufferedBlockNum(blockNum);
      size = 0;
      blockNum = 0;
    }
  }

  public StatusCode append(ShufflePartitionedData data) {
    int partition = data.getPartitionId();
    List<ShufflePartitionedBlock> cur = partitionBuffers.get(partition);

    int mSize = 0;
    int mNum = 0;
    for (ShufflePartitionedBlock block : data.getBlockList()) {
      cur.add(block);
      mSize += block.size();
      mNum += 1;
      size += mSize;
      blockNum += 1;
    }

    ShuffleServerMetrics.incBufferedBlockNum(mNum);
    ShuffleServerMetrics.incBufferedBlockSize(mSize);

    return StatusCode.SUCCESS;
  }

  public List<ShufflePartitionedBlock> getBlocks(int partition) {
    return partitionBuffers.get(partition);
  }

  public int getSize() {
    return this.size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getBlockNum() {
    return this.blockNum;
  }

  public int getPartitionNum() {
    return this.partitionBuffers.size();
  }

  public boolean full() {
    return this.size > capacity;
  }

  @VisibleForTesting
  int getCapacity() {
    return this.capacity;
  }
}
