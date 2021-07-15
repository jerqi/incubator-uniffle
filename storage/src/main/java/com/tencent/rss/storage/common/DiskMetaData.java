package com.tencent.rss.storage.common;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private final Map<String, ShuffleMeta> shuffleMetaMap = Maps.newConcurrentMap();


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
    return getShuffleMeta(shuffleId).getSize().addAndGet(delta);
  }

  public long updateUploadedShuffleSize(String shuffleId, long delta) {
    return getShuffleMeta(shuffleId).getUploadedSize().addAndGet(delta);
  }

  public void updateShufflePartitionList(String shuffleId, List<Integer> partitions) {
    getShuffleMeta(shuffleId).getPartitionSet().addAll(partitions);
  }

  public void updateUploadedShufflePartitionList(String shuffleId, List<Integer> partitions) {
    getShuffleMeta(shuffleId).getUploadedPartitionSet().addAll(partitions);
  }

  public void setHasRead(String shuffleId) {
    getShuffleMeta(shuffleId).getHasRead().set(true);
  }

  public void removeShufflePartitionList(String shuffleId, List<Integer> partitions) {
    getShuffleMeta(shuffleId).getPartitionSet().removeAll(partitions);
  }

  public void removeUploadedShufflePartitionList(String shuffleId, List<Integer> partitions) {
    getShuffleMeta(shuffleId).getUploadedPartitionSet().removeAll(partitions);
  }

  public void remoteShuffle(String shuffleId) {
    shuffleMetaMap.remove(shuffleId);
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

  public long getShuffleSize(String shuffleKey) {
    return getShuffleMeta(shuffleKey).getSize().get();
  }

  public long getShuffleUploadedSize(String shuffleKey) {
    return getShuffleMeta(shuffleKey).getUploadedSize().get();
  }

  public boolean getShuffleHasRead(String shuffleKey) {
    return getShuffleMeta(shuffleKey).getHasRead().get();
  }

  public Set<String> getShuffleMetaSet() {
    return shuffleMetaMap.keySet();
  }

  /**
   *  If the method is implemented as below:
   *
   *     if (shuffleMetaMap.contains(shuffleId)) {
   *        // `Time A`
   *        return shuffleMetaMap.get(shuffleId)
   *     } else {
   *        shuffleMetaMap.putIfAbsent(shuffleId, newMeta)
   *        return newMeta
   *    }
   *
   *  Because if shuffleMetaMap remove shuffleId at `Time A` in another thread,
   *  shuffleMetaMap.get(shuffleId) will return null.
   *  We need to guarantee that this method is thread safe, and won't return null.
   **/
  private ShuffleMeta getShuffleMeta(String shuffleKey) {
    ShuffleMeta meta = new ShuffleMeta();
    ShuffleMeta oldMeta = shuffleMetaMap.putIfAbsent(shuffleKey, meta);
    return (oldMeta == null) ? meta : oldMeta;
  }

  // Consider that ShuffleMeta is a simple class, we keep the class ShuffleMeta as
  // an inner class.
  private class ShuffleMeta {
    private final AtomicLong size = new AtomicLong(0);
    private final Set<Integer> partitionSet = Sets.newConcurrentHashSet();
    private final Set<Integer> uploadedPartitionSet = Sets.newConcurrentHashSet();
    private final AtomicLong uploadedSize = new AtomicLong(0);
    private final AtomicBoolean hasRead = new AtomicBoolean(false);

    public AtomicLong getSize() {
      return size;
    }

    public AtomicLong getUploadedSize() {
      return uploadedSize;
    }

    public Set<Integer> getPartitionSet() {
      return partitionSet;
    }

    public Set<Integer> getUploadedPartitionSet() {
      return uploadedPartitionSet;
    }

    public AtomicBoolean getHasRead() {
      return hasRead;
    }
  }

}
