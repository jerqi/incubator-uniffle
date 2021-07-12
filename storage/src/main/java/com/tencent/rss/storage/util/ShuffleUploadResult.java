package com.tencent.rss.storage.util;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ShuffleUploadResult {
  private String shuffleKey;
  private long size;
  private List<Integer> partitions;

  public ShuffleUploadResult() {
    this.size = 0;
    partitions = null;
  }

  public ShuffleUploadResult(long size, List<Integer> partitions) {
    this.size = size;
    this.partitions = partitions;
  }

  public String getShuffleKey() {
    return shuffleKey;
  }

  public void setShuffleKey(String shuffleKey) {
    this.shuffleKey = shuffleKey;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public List<Integer> getPartitions() {
    return partitions;
  }

  public void setPartitions(List<Integer> partitions) {
    this.partitions = partitions;
  }

  public static ShuffleUploadResult merge(List<ShuffleUploadResult> results) {
    if (results == null || results.isEmpty()) {
      return null;
    }

    long size = results.stream().map(ShuffleUploadResult::getSize).reduce(0L, Long::sum);
    List<Integer> partitions = results
        .stream()
        .map(ShuffleUploadResult::getPartitions)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
    return new ShuffleUploadResult(size, partitions);
  }

}
