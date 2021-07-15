package com.tencent.rss.common;

import java.util.List;

public class ShuffleRegisterInfo {

  private ShuffleServerInfo shuffleServerInfo;
  private List<PartitionRange> partitionRanges;

  public ShuffleRegisterInfo(ShuffleServerInfo shuffleServerInfo, List<PartitionRange> partitionRanges) {
    this.shuffleServerInfo = shuffleServerInfo;
    this.partitionRanges = partitionRanges;
  }

  public ShuffleServerInfo getShuffleServerInfo() {
    return shuffleServerInfo;
  }

  public List<PartitionRange> getPartitionRanges() {
    return partitionRanges;
  }

  @Override
  public int hashCode() {
    return shuffleServerInfo.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ShuffleRegisterInfo) {
      return shuffleServerInfo.equals(((ShuffleRegisterInfo) obj).getShuffleServerInfo())
          && partitionRanges.equals(((ShuffleRegisterInfo) obj).getPartitionRanges());
    }
    return false;
  }

  @Override
  public String toString() {
    return "ShuffleRegisterInfo: shuffleServerInfo[" + shuffleServerInfo.getId() + "], " + partitionRanges;
  }
}
