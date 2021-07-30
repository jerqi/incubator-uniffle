package com.tencent.rss.storage.common;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleFileInfo {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleFileInfo.class);

  private final List<File> dataFiles = Lists.newLinkedList();
  private final List<File> indexFiles = Lists.newLinkedList();
  private final List<Integer> partitions = Lists.newLinkedList();
  private String key;
  private long size;

  public boolean isValid() {
    if (key == null || key.isEmpty()) {
      LOG.error("Shuffle key is null or empty");
      return false;
    }

    if (size <= 0) {
      LOG.error("Total size of shuffle [{}]", key);
      return false;
    }

    if (dataFiles.isEmpty() || indexFiles.isEmpty() || partitions.isEmpty()) {
      LOG.error(
          "Data files num {}, index files num {} and partition files num {} is invalid",
          dataFiles.size(),
          indexFiles.size(),
          partitions.size());
      return false;
    }

    if ((dataFiles.size() != indexFiles.size()) || (dataFiles.size() != partitions.size())) {
      LOG.error(
          "Data files num {}, index files num {} and partition files num {} are not the same",
          dataFiles.size(),
          indexFiles.size(),
          partitions.size());
      return false;
    }

    return true;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public List<File> getDataFiles() {
    return dataFiles;
  }

  public List<File> getIndexFiles() {
    return indexFiles;
  }

  public List<Integer> getPartitions() {
    return partitions;
  }



  public long getSize() {
    return size;
  }

  public boolean shouldCombine(long uploadCombineThresholdMB) {
    return size / dataFiles.size() / 1024 / 1024 < uploadCombineThresholdMB;
  }

  public boolean isEmpty() {
    return dataFiles.isEmpty();
  }

  public String getKey() {
    return key;
  }

}
