package com.tencent.rss.server;

import com.google.common.collect.Lists;
import com.tencent.rss.storage.common.DiskItem;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MultiStorageManager {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStorageManager.class);
  private final List<String> dirs;
  private final long capacity;
  private final double cleanupThreshold;
  private final double highWaterMarkOfWrite;
  private final double lowWaterMarkOfWrite;
  private final List<DiskItem> diskItems = Lists.newArrayList();

  public MultiStorageManager(
      List<String> dirs,
      long capacity,
      double cleanupThreshold,
      double highWaterMarkOfWrite,
      double lowWaterMarkOfWrite) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("Capacity must be larger than zero");
    }
    if (cleanupThreshold < 0 || cleanupThreshold > 100) {
      throw new IllegalArgumentException("cleanupThreshold must be between 0 and 100");
    }
    if (highWaterMarkOfWrite < lowWaterMarkOfWrite) {
      throw new IllegalArgumentException("highWaterMarkOfWrite must be larger than lowWaterMarkOfWrite");
    }
    if (lowWaterMarkOfWrite < 0) {
      throw new IllegalArgumentException("lowWaterMarkOfWrite must be larger than zero");
    }
    if (highWaterMarkOfWrite > 100) {
      throw new IllegalArgumentException("highWaterMarkOfWrite must be smaller than 100");
    }
    this.dirs = dirs;
    this.capacity = capacity;
    this.cleanupThreshold = cleanupThreshold;
    this.highWaterMarkOfWrite = highWaterMarkOfWrite;
    this.lowWaterMarkOfWrite = lowWaterMarkOfWrite;
    initialize();
  }

  void initialize() throws RuntimeException {
    // TODO: 1.adapt to heterogeneous env and config different capacity for each disk item
    //       2.each total capacity and server buffer size,
    for (String dir : dirs) {
      // todo: if there is a disk is corrupted, we should skip. now shuffleServer will
      // crash.
      DiskItem item = new DiskItem(
          dir, cleanupThreshold, highWaterMarkOfWrite, lowWaterMarkOfWrite, 100, 5000);
      diskItems.add(item);
    }
  }

  public boolean canWrite(ShuffleDataFlushEvent event) {
    DiskItem diskItem = getDiskItem(event);
    return diskItem.canWrite();
  }

  public void updateWriteEvent(ShuffleDataFlushEvent event) {
    DiskItem diskItem = getDiskItem(event);
    String appId = event.getAppId();
    int shuffleId = event.getShuffleId();
    int partitionId = event.getStartPartition();
    // TODO: use appId, shuffleId, partitionId to update metadata in diskItem
    String key = generateKey(appId, shuffleId);
    diskItem.updateWrite(key, event.getSize());
  }

  public void updateReadEvent(String appId, int shuffleId, int partitionId, long size) {
    DiskItem diskItem = getDiskItem(appId, shuffleId, partitionId);
    // TODO: use appId, shuffleId, partitionId to update metadata in diskItem
    String key = generateKey(appId, shuffleId);
    diskItem.updateRead(key, size);
  }

  public DiskItem getDiskItem(ShuffleDataFlushEvent event) {
    // TODO: add exception handling and LOG
    return getDiskItem(event.getAppId(), event.getShuffleId(), event.getStartPartition());
  }

  public DiskItem getDiskItem(String appId, int shuffleId, int partitionId) {
    // TODO: add exception handling and LOG
    int dirId = getDiskItemId(appId, shuffleId, partitionId);
    return diskItems.get(dirId);
  }

  public String generateKey(String appId, int shuffleId) {
    return String.join("/", appId, String.valueOf(shuffleId));
  }

  public String generateKey(ShuffleDataFlushEvent event) {
    return generateKey(event.getAppId(), (event.getShuffleId()));
  }

  public int getDiskItemId(String appId, int shuffleId, int partitionId) {
    return ShuffleStorageUtils.getStorageIndex(diskItems.size(), appId, shuffleId, partitionId);
  }

  public String generateDir(String appId, int shuffleId, int partitionId) {
    return String.join(
        "/", appId, String.valueOf(shuffleId), String.valueOf(partitionId));
  }

  public String generateDir(ShuffleDataFlushEvent event) {
    return generateDir(event.getAppId(), event.getShuffleId(), event.getStartPartition());
  }

  public void removeResources(String shuffleKey) {
    diskItems.forEach(item -> item.removeResources(shuffleKey));
  }
}
