package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

import java.util.List;

public class ShuffleIndexHeader {

  private int partitionNum;
  private final List<Entry> indexes = Lists.newArrayList();
  private long crc;

  public void setPartitionNum(int partitionNum) {
    this.partitionNum = partitionNum;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public List<Entry> getIndexes() {
    return indexes;
  }

  public long getCrc() {
    return crc;
  }

  public void setCrc(long crc) {
    this.crc = crc;
  }

  public int getHeaderLen() {
    return (int)ShuffleStorageUtils.getIndexFileHeaderLen(partitionNum);
  }

  static class Entry {
    Integer partitionId;
    Long partitionIndexLength;
    Long partitionDataLength;

    Entry(Integer partitionId, Long partitionIndexLength, long partitionDataLength) {
      this.partitionId = partitionId;
      this.partitionIndexLength = partitionIndexLength;
      this.partitionDataLength = partitionDataLength;
    }

    public Integer getPartitionId() {
      return partitionId;
    }

    public Long getPartitionIndexLength() {
      return partitionIndexLength;
    }

    public Long getPartitionDataLength() {
      return partitionDataLength;
    }
  }
}
