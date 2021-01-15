package com.tecent.rss.client.request;

import java.util.Set;
import org.apache.hadoop.conf.Configuration;

public class CreateShuffleReadClientRequest {

  private String storageType;
  private String basePath;
  private Configuration hadoopConf;
  private int indexReadLimit;
  private Set<Long> expectedBlockIds;

  public CreateShuffleReadClientRequest(String storageType, String basePath,
      Configuration hadoopConf, int indexReadLimit, Set<Long> expectedBlockIds) {
    this.storageType = storageType;
    this.basePath = basePath;
    this.hadoopConf = hadoopConf;
    this.indexReadLimit = indexReadLimit;
    this.expectedBlockIds = expectedBlockIds;
  }

  public String getStorageType() {
    return storageType;
  }

  public String getBasePath() {
    return basePath;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public int getIndexReadLimit() {
    return indexReadLimit;
  }

  public Set<Long> getExpectedBlockIds() {
    return expectedBlockIds;
  }
}
