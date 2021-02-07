package com.tencent.rss.storage.request;

import org.apache.hadoop.conf.Configuration;

public class CreateShuffleWriteHandlerRequest {

  private String storageType;
  private String appId;
  private int shuffleId;
  private int startPartition;
  private int endPartition;
  private String storageBasePath;
  private String fileNamePrefix;
  private Configuration conf;

  public CreateShuffleWriteHandlerRequest(String storageType, String appId, int shuffleId,
      int startPartition, int endPartition, String storageBasePath, String fileNamePrefix, Configuration conf) {
    this.storageType = storageType;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.storageBasePath = storageBasePath;
    this.fileNamePrefix = fileNamePrefix;
    this.conf = conf;
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

  public int getStartPartition() {
    return startPartition;
  }

  public int getEndPartition() {
    return endPartition;
  }

  public String getStorageBasePath() {
    return storageBasePath;
  }

  public String getFileNamePrefix() {
    return fileNamePrefix;
  }

  public Configuration getConf() {
    return conf;
  }
}
