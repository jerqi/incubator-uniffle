package com.tencent.rss.storage.common;

public class ShuffleInfo {
  private String key;
  private long size;

  public ShuffleInfo(String key, long size) {
    this.key = key;
    this.size = size;
  }

  public String getKey() {
    return key;
  }

  public long getSize() {
    return size;
  }
}