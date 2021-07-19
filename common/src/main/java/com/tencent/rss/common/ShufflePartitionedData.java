package com.tencent.rss.common;

public class ShufflePartitionedData {

  private int partitionId;
  private ShufflePartitionedBlock[] blockList;

  public ShufflePartitionedData(int partitionId, ShufflePartitionedBlock[] blockList) {
    this.partitionId = partitionId;
    this.blockList = blockList;
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

  public ShufflePartitionedBlock[] getBlockList() {
    if (blockList == null) {
      return new ShufflePartitionedBlock[]{};
    }
    return blockList;
  }

}
