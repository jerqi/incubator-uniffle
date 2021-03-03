package com.tencent.rss.common;

import java.util.List;

public class ShuffleBlockInfo {

  private int partitionId;
  private long blockId;
  private int length;
  private int shuffleId;
  private long crc;
  private byte[] data;
  private List<ShuffleServerInfo> shuffleServerInfos;
  private int uncompressLength;

  public ShuffleBlockInfo(int shuffleId, int partitionId, long blockId, int length, long crc,
      byte[] data, List<ShuffleServerInfo> shuffleServerInfos, int uncompressLength) {
    this.partitionId = partitionId;
    this.blockId = blockId;
    this.length = length;
    this.crc = crc;
    this.data = data;
    this.shuffleId = shuffleId;
    this.shuffleServerInfos = shuffleServerInfos;
    this.uncompressLength = uncompressLength;
  }

  public long getBlockId() {
    return blockId;
  }

  public int getLength() {
    return length;
  }

  public long getCrc() {
    return crc;
  }

  public byte[] getData() {
    return data;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public List<ShuffleServerInfo> getShuffleServerInfos() {
    return shuffleServerInfos;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ShuffleBlockInfo:");
    sb.append("shuffleId[" + shuffleId + "],");
    sb.append("partitionId[" + partitionId + "],");
    sb.append("blockId[" + blockId + "],");
    sb.append("length[" + length + "],");
    sb.append("uncompressLength[" + uncompressLength + "],");
    sb.append("crc[" + crc + "],");
    if (shuffleServerInfos != null) {
      sb.append("shuffleServer[");
      for (ShuffleServerInfo ssi : shuffleServerInfos) {
        sb.append(ssi.getId() + ",");
      }
      sb.append("]");
    } else {
      sb.append("shuffleServer is empty");
    }

    return sb.toString();
  }
}
