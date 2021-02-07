package com.tencent.rss.storage.request;

import java.util.Set;

public class CreateShuffleReadHandlerRequest {

  private String storageType;
  private String appId;
  private int shuffleId;
  private int partitionId;
  private int indexReadLimit;
  private int partitionsPerServer;
  private int partitionNum;
  private int readBufferSize;
  private String storageBasePath;
  private Set<Long> expectedBlockIds;

  public CreateShuffleReadHandlerRequest(String storageType, String appId, int shuffleId, int partitionId,
      int indexReadLimit, int partitionsPerServer, int partitionNum, int readBufferSize, String storageBasePath,
      Set<Long> expectedBlockIds) {
    this.storageType = storageType;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionsPerServer = partitionsPerServer;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.storageBasePath = storageBasePath;
    this.expectedBlockIds = expectedBlockIds;
  }

  public String getStorageType() {
    return storageType;
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

  public int getIndexReadLimit() {
    return indexReadLimit;
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

  public String getStorageBasePath() {
    return storageBasePath;
  }

  public Set<Long> getExpectedBlockIds() {
    return expectedBlockIds;
  }
}
