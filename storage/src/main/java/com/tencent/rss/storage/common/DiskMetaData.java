package com.tencent.rss.storage.common;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Metadata has three dimensions from top to down including disk, shuffle, partition.
 *  And each dimensions contains two aspects, status data and indicator data.
 *  Disk status data contains writable flag, Shuffle status data contains stable, uploading, deleting flag.
 *  Disk indicator data contains size, fileNum, shuffleNum, Shuffle indicator contains size, partition list,
 *  uploaded partition list and uploaded size.
 */
public class DiskMetaData {

  private static final Logger LOG = LoggerFactory.getLogger(DiskMetaData.class);
  private final AtomicLong size = new AtomicLong(0L);
  private final Map<String, ShuffleMeta> shuffleMetaMap = Maps.newConcurrentMap();

  // todo: add ut
  public List<String> getSortedShuffleKeys(boolean checkRead, int hint) {
    // Filter the unread shuffle is checkRead is true
    // Filter the remain size is 0
    List<Map.Entry<String, ShuffleMeta>> shuffleMetaList = shuffleMetaMap
        .entrySet()
        .stream()
        .filter(e -> (!checkRead || e.getValue().isStartRead.get()) && e.getValue().getNotUploadedSize() > 0)
        .collect(Collectors.toList());

      shuffleMetaList.sort((Entry<String, ShuffleMeta> o1, Entry<String, ShuffleMeta> o2) -> {
        long sz1 = o1.getValue().getSize().longValue();
        long sz2 = o2.getValue().getSize().longValue();
        return -Long.compare(sz1, sz2);
      });

    return shuffleMetaList
        .subList(0, Math.min(shuffleMetaList.size(), hint))
        .stream()
        .map(Entry::getKey).collect(Collectors.toList());
  }

  public RoaringBitmap getNotUploadedPartitions(String shuffleKey) {
    ShuffleMeta shuffleMeta = getShuffleMeta(shuffleKey);
    RoaringBitmap partitionBitmap;
    RoaringBitmap uploadedPartitionBitmap;
    synchronized (shuffleMeta.partitionBitmap) {
      partitionBitmap = shuffleMeta.partitionBitmap.clone();
    }
    synchronized (shuffleMeta.uploadedPartitionBitmap) {
      uploadedPartitionBitmap = shuffleMeta.uploadedPartitionBitmap.clone();
    }
    for (int partition : uploadedPartitionBitmap) {
        partitionBitmap.remove(partition);
    }
    return partitionBitmap;
  }

  public long getNotUploadedSize(String shuffleKey) {
    return getShuffleMeta(shuffleKey).getNotUploadedSize();
  }

  public long updateDiskSize(long delta) {
    return size.addAndGet(delta);
  }

  public long updateShuffleSize(String shuffleId, long delta) {
    return getShuffleMeta(shuffleId).getSize().addAndGet(delta);
  }

  public long updateUploadedShuffleSize(String shuffleKey, long delta) {
    return getShuffleMeta(shuffleKey).uploadedSize.addAndGet(delta);
  }

  public void addShufflePartitionList(String shuffleKey, List<Integer> partitions) {
    RoaringBitmap bitmap = getShuffleMeta(shuffleKey).partitionBitmap;
    synchronized (bitmap) {
      partitions.forEach(p -> bitmap.add(p));
    }
  }

  public void addUploadedShufflePartitionList(String shuffleKey, List<Integer> partitions) {
    RoaringBitmap bitmap = getShuffleMeta(shuffleKey).uploadedPartitionBitmap;
    synchronized (bitmap) {
      partitions.forEach(p -> bitmap.add(p));
    }
  }

  public void prepareStartRead(String shuffleId) {
    getShuffleMeta(shuffleId).markStartRead();
  }

  public void removeShufflePartitionList(String shuffleKey, List<Integer> partitions) {
    RoaringBitmap bitmap = getShuffleMeta(shuffleKey).partitionBitmap;
    synchronized (bitmap) {
      partitions.forEach(p -> bitmap.remove(p));
    }
  }

  public void remoteShuffle(String shuffleKey) {
    shuffleMetaMap.remove(shuffleKey);
  }

  public AtomicLong getDiskSize() {
    return size;
  }

  public long getShuffleSize(String shuffleKey) {
    return getShuffleMeta(shuffleKey).getSize().get();
  }

  public boolean isShuffleStartRead(String shuffleKey) {
    return getShuffleMeta(shuffleKey).isStartRead();
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

  public long getShuffleLastReadTs(String shuffleKey) {
    return getShuffleMeta(shuffleKey).getShuffleLastReadTs();
  }

  public void updateShuffleLastReadTs(String shuffleKey) {
    getShuffleMeta(shuffleKey).updateLastReadTs();
  }

  public ReadWriteLock getLock(String shuffleKey) {
    return getShuffleMeta(shuffleKey).getLock();
  }

  // Consider that ShuffleMeta is a simple class, we keep the class ShuffleMeta as an inner class.
  private class ShuffleMeta {
    private final AtomicLong size = new AtomicLong(0);
    private final RoaringBitmap partitionBitmap = RoaringBitmap.bitmapOf();
    private final AtomicLong uploadedSize = new AtomicLong(0);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean isStartRead = new AtomicBoolean(false);
    private final RoaringBitmap uploadedPartitionBitmap = RoaringBitmap.bitmapOf();
    private AtomicLong lastReadTs = new AtomicLong(-1L);

    public AtomicLong getSize() {
      return size;
    }

    public AtomicLong getUploadedSize() {
      return uploadedSize;
    }

    public long getNotUploadedSize() {
      return size.longValue() - uploadedSize.longValue();
    }

    public boolean isStartRead() {
      return isStartRead.get();
    }

    public void markStartRead() {
      isStartRead.set(true);
    }

    public void updateLastReadTs() {
      lastReadTs.set(System.currentTimeMillis());
    }


    public long getShuffleLastReadTs() {
      return lastReadTs.get();
    }

    public ReadWriteLock getLock() {
      return lock;
    }
  }
}
