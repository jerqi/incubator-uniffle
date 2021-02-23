package com.tencent.rss.client.request;

import java.util.Set;

public class RssGetShuffleDataRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private int partitionsPerServer;
  private int partitionNum;
  private int readBufferSize;
  private Set<Long> blockIds;

  public RssGetShuffleDataRequest(String appId, int shuffleId, int partitionId, int partitionsPerServer,
      int partitionNum,
      int readBufferSize, Set<Long> blockIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.partitionsPerServer = partitionsPerServer;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.blockIds = blockIds;
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public int getPartitionsPerServer() {
    return partitionsPerServer;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public Set<Long> getBlockIds() {
    return blockIds;
  }
}