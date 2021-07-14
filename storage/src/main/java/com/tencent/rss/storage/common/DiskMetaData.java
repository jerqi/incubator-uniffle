package com.tencent.rss.storage.common;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Metadata has three dimensions from top to down including disk, shuffle, partition.
 *  And each dimensions contains two aspects, status data and indicator data.
 *  Disk status data contains writable flag, Shuffle status data contains stable, uploading, deleting flag.
 *  Disk indicator data contains size, fileNum, shuffleNum, Shuffle indicator contains size, partition list,
 *  uploaded partition list and uploaded size.
 *
 */
public class DiskMetaData {

  private static final Logger LOG = LoggerFactory.getLogger(DiskMetaData.class);

  private final AtomicLong size = new AtomicLong(0L);
  private final AtomicLong fileNum = new AtomicLong(0L);
  private final AtomicLong shuffleNum = new AtomicLong(0L);

  private final Map<String, AtomicLong> shuffleSize = Maps.newConcurrentMap();
  private final Map<String, Set<Integer>> shufflePartitionList = Maps.newConcurrentMap();
  private final Map<String, Set<Integer>> uploadedShufflePartitionList = Maps.newConcurrentMap();
  private final Map<String, AtomicLong> uploadedShuffleSize = Maps.newConcurrentMap();

  public long updateDiskSize(long delta) {
    return size.addAndGet(delta);
  }

  public long updateFileNum(long delta) {
    return fileNum.addAndGet(delta);
  }

  public long updateShuffleNum(long delta) {
    return shuffleNum.addAndGet(delta);
  }

  public long updateShuffleSize(String shuffleId, long delta) {
    return updateSizeInternal(shuffleSize, shuffleId, delta);
  }

  public long updateUploadedShuffleSize(String shuffleId, long delta) {
    return updateSizeInternal(uploadedShuffleSize, shuffleId, delta);
  }

  public void updateShufflePartitionList(String shuffleId, List<Integer> partitions) {
    addPartitionsInternal(shufflePartitionList, shuffleId, partitions);
  }

  public void updateUploadedShufflePartitionList(String shuffleId, List<Integer> partitions) {
    addPartitionsInternal(uploadedShufflePartitionList, shuffleId, partitions);
  }

  public void removeShufflePartitionList(String shuffleId, List<Integer> partitions) {
    removePartitionsInternal(shufflePartitionList, shuffleId, partitions);
  }

  public void removeUploadedShufflePartitionList(String shuffleId, List<Integer> partitions) {
    removePartitionsInternal(uploadedShufflePartitionList, shuffleId, partitions);
  }

  public void remoteShuffle(String shuffleId) {
    shuffleSize.remove(shuffleId);
    shufflePartitionList.remove(shuffleId);
    uploadedShufflePartitionList.remove(shuffleId);
    uploadedShuffleSize.remove(shuffleId);
  }

  public AtomicLong getDiskSize() {
    return size;
  }

  public AtomicLong getFileNum() {
    return fileNum;
  }

  public AtomicLong getShuffleNum() {
    return shuffleNum;
  }

  public Map<String, AtomicLong> getShuffleSize() {
    return shuffleSize;
  }

  public Map<String, Set<Integer>> getShufflePartitionList() {
    return shufflePartitionList;
  }

  public Map<String, Set<Integer>> getUploadedShufflePartitionList() {
    return uploadedShufflePartitionList;
  }

  public Map<String, AtomicLong> getUploadedShuffleSize() {
    return uploadedShuffleSize;
  }

  private long updateSizeInternal(Map<String, AtomicLong> map, String key, long delta) {
    if (map.containsKey(key)) {
      return map.get(key).addAndGet(delta);
    } else {
      map.putIfAbsent(key, new AtomicLong(0));
      return map.get(key).addAndGet(delta);
    }
  }

  private void addPartitionsInternal(Map<String, Set<Integer>> map, String key, List<Integer> partitions) {
    if (map.containsKey(key)) {
      map.get(key).addAll(partitions);
    } else {
      map.putIfAbsent(key, Sets.newConcurrentHashSet());
      map.get(key).addAll(partitions);
    }
  }

  private void removePartitionsInternal(Map<String, Set<Integer>> map, String key, List<Integer> partitions) {
    if (map.containsKey(key)) {
      LOG.error("{} dont exist", key);
      return;
    }

    map.get(key).removeAll(partitions);
  }

}
