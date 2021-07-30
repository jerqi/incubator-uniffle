package com.tencent.rss.client.request;

public class RssGetShuffleDataRequest {

  private String appId;
  private int shuffleId;
  private int partitionId;
  private int partitionNumPerRange;
  private int partitionNum;
  private int readBufferSize;
  private int segmentIndex;

  public RssGetShuffleDataRequest(String appId, int shuffleId, int partitionId, int partitionNumPerRange,
      int partitionNum, int readBufferSize, int segmentIndex) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.segmentIndex = segmentIndex;
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

  public int getPartitionNumPerRange() {
    return partitionNumPerRange;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public int getSegmentIndex() {
    return segmentIndex;
  }
}
