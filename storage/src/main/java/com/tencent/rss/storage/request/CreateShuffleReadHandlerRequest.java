package com.tencent.rss.storage.request;

import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.config.RssBaseConf;
import java.util.List;
import java.util.Set;

public class CreateShuffleReadHandlerRequest {

  private String storageType;
  private String appId;
  private int shuffleId;
  private int partitionId;
  private int indexReadLimit;
  private int partitionNumPerRange;
  private int partitionNum;
  private int readBufferSize;
  private String storageBasePath;
  private Set<Long> expectedBlockIds;
  private RssBaseConf rssBaseConf;
  private List<ShuffleServerInfo> shuffleServerInfoList;

  public CreateShuffleReadHandlerRequest() {
  }

  public RssBaseConf getRssBaseConf() {
    return rssBaseConf;
  }

  public void setRssBaseConf(RssBaseConf rssBaseConf) {
    this.rssBaseConf = rssBaseConf;
  }

  public String getStorageType() {
    return storageType;
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public void setShuffleId(int shuffleId) {
    this.shuffleId = shuffleId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public int getIndexReadLimit() {
    return indexReadLimit;
  }

  public void setIndexReadLimit(int indexReadLimit) {
    this.indexReadLimit = indexReadLimit;
  }

  public int getPartitionNumPerRange() {
    return partitionNumPerRange;
  }

  public void setPartitionNumPerRange(int partitionNumPerRange) {
    this.partitionNumPerRange = partitionNumPerRange;
  }

  public int getPartitionNum() {
    return partitionNum;
  }

  public void setPartitionNum(int partitionNum) {
    this.partitionNum = partitionNum;
  }

  public int getReadBufferSize() {
    return readBufferSize;
  }

  public void setReadBufferSize(int readBufferSize) {
    this.readBufferSize = readBufferSize;
  }

  public String getStorageBasePath() {
    return storageBasePath;
  }

  public void setStorageBasePath(String storageBasePath) {
    this.storageBasePath = storageBasePath;
  }

  public Set<Long> getExpectedBlockIds() {
    return expectedBlockIds;
  }

  public void setExpectedBlockIds(Set<Long> expectedBlockIds) {
    this.expectedBlockIds = expectedBlockIds;
  }

  public List<ShuffleServerInfo> getShuffleServerInfoList() {
    return shuffleServerInfoList;
  }

  public void setShuffleServerInfoList(List<ShuffleServerInfo> shuffleServerInfoList) {
    this.shuffleServerInfoList = shuffleServerInfoList;
  }
}
