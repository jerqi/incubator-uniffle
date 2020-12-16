package com.tencent.rss.common;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ShufflePartitionedData {

  private int partitionId;
  private List<ShufflePartitionedBlock> blockList;

  public ShufflePartitionedData(int partitionId, List<ShufflePartitionedBlock> blockList) {
    this.partitionId = partitionId;
    this.blockList = blockList;
  }

  public ShufflePartitionedData(int partitionId, ShufflePartitionedBlock blockList) {
    this.partitionId = partitionId;
    Objects.requireNonNull(blockList);
    this.blockList = Collections.singletonList(blockList);
  }

  @Override
  public String toString() {
    return "ShufflePartitionedData{partitionId=" + partitionId + ", blockList=" + blockList + '}';
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public List<ShufflePartitionedBlock> getBlockList() {
    return blockList;
  }

  public void setBlockList(List<ShufflePartitionedBlock> blockList) {
    this.blockList = blockList;
  }
}
