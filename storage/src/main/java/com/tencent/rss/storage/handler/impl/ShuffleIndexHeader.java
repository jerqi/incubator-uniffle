package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Queues;
import java.util.Queue;

public class ShuffleIndexHeader {

  private int partitionNum;
  private final Queue<Entry> indexes = Queues.newArrayDeque();
  private long crc;

  public void setPartitionNum(int partitionNum) {
    this.partitionNum = partitionNum;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public Queue<Entry> getIndexes() {
    return indexes;
  }

  public long getCrc() {
    return crc;
  }

  public void setCrc(long crc) {
    this.crc = crc;
  }

  public int getHeaderLen() {
    return 4 + (4 + 8) * partitionNum + 8;
  }

  static class Entry {
    Integer key;
    Long value;

    Entry(Integer key, Long value) {
      this.key = key;
      this.value = value;
    }

    public Integer getKey() {
      return key;
    }

    public Long getValue() {
      return value;
    }
  }
}
