package com.tencent.rss.storage.request;

import org.apache.hadoop.conf.Configuration;

public class CreateShuffleDeleteHandlerRequest {

  private String storageType;
  private Configuration conf;

  public CreateShuffleDeleteHandlerRequest(String storageType, Configuration conf) {
    this.storageType = storageType;
    this.conf = conf;
  }

  public String getStorageType() {
    return storageType;
  }

  public Configuration getConf() {
    return conf;
  }
}
