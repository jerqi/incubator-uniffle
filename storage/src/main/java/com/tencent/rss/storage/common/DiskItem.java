package com.tencent.rss.storage.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DiskItem {

  private static final Logger LOG = LoggerFactory.getLogger(DiskMetaData.class);

  private final long capacity;
  private final String basePath;
  private final double cleanupThreshold;
  private final double highWaterMarkOfWrite;
  private final double lowWaterMarkOfWrite;

  private DiskMetaData diskMetaData = new DiskMetaData();
  private boolean canRead = true;

  private DiskItem(Builder builder) {
    this.basePath = builder.basePath;
    this.cleanupThreshold = builder.cleanupThreshold;
    this.highWaterMarkOfWrite = builder.highWaterMarkOfWrite;
    this.lowWaterMarkOfWrite = builder.lowWaterMarkOfWrite;
    this.capacity = builder.capacity;

    File baseFolder = new File(basePath);
    try {
      FileUtils.deleteDirectory(baseFolder);
      baseFolder.mkdirs();
    } catch (IOException ioe) {
      LOG.warn("Init base directory " + basePath + " fail, the disk should be corrupted", ioe);
      throw new RuntimeException(ioe);
    }
    long freeSpace = baseFolder.getFreeSpace();
    if (freeSpace < capacity) {
      throw new IllegalArgumentException("Disk Available Capacity " + freeSpace
          + " is smaller than configuration");
    }

    // todo: extract a class named Service, and support stop method. Now
    // we assume that it's enough for one thread per disk. If in testing mode
    // the thread won't be started. cleanInterval should minus the execute time
    // of the method clean.
    Thread thread = new Thread(basePath + "-Cleaner") {
      @Override
      public void run() {
        for (;;) {
          try {
            clean();
            // todo: get sleepInterval from configuration
            Uninterruptibles.sleepUninterruptibly(builder.cleanIntervalMs, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            LOG.error(getName() + " happened exception: ", e);
          }
        }
      }
    };
    thread.setDaemon(true);
    thread.start();
  }

  public boolean canWrite() {
    if (canRead) {
      canRead = diskMetaData.getDiskSize().doubleValue() * 100 / capacity < highWaterMarkOfWrite;
    } else {
      canRead = diskMetaData.getDiskSize().doubleValue() * 100 / capacity < lowWaterMarkOfWrite;
    }
    return canRead;
  }

  public String getBasePath() {
    return basePath;
  }

  public void updateWrite(String shuffleKey, long delta) {
    updateWrite(shuffleKey, delta, Lists.newArrayList());
  }

  public void updateWrite(String shuffleKey, long delta, List<Integer> partitionList) {
    diskMetaData.updateDiskSize(delta);
    diskMetaData.updateShuffleSize(shuffleKey, delta);
    diskMetaData.updateShufflePartitionList(shuffleKey, partitionList);
  }

  public void updateRead(String key) {
    diskMetaData.setHasRead(key);
  }

  // todo: refactor DeleteHandler to support shuffleKey level deletion
  @VisibleForTesting
  void clean() {
    if (diskMetaData.getDiskSize().doubleValue() * 100 / capacity < cleanupThreshold) {
      return;
    }
    diskMetaData.getShuffleMetaSet().forEach((shuffleKey) -> {
      // If shuffle data is started to read, shuffle data won't be appended. When shuffle is
      // uploaded totally, the partitions which is not uploaded is empty.
      if (diskMetaData.getShuffleHasRead(shuffleKey)
          && diskMetaData.getNotUploadedPartitions(shuffleKey).isEmpty()) {
        String shufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(basePath, shuffleKey);
        long start = System.currentTimeMillis();
        try {
          File baseFolder = new File(shufflePath);
          FileUtils.deleteDirectory(baseFolder);
          removeResources(shuffleKey);
          LOG.info("Delete shuffle data for shuffle [" + shuffleKey + "] with " + shufflePath
              + " cost " + (System.currentTimeMillis() - start) + " ms");
        } catch (Exception e) {
          LOG.warn("Can't delete shuffle data for shuffle [" + shuffleKey + "] with " + shufflePath, e);
        }
      }
    });

  }

  public static class Builder {
    private long capacity;
    private double lowWaterMarkOfWrite;
    private double highWaterMarkOfWrite;
    private double cleanupThreshold;
    private String basePath;
    private long cleanIntervalMs;

    private Builder() {
    }

    public Builder capacity(long capacity) {
      this.capacity = capacity;
      return this;
    }

    public Builder lowWaterMarkOfWrite(double lowWaterMarkOfWrite) {
      this.lowWaterMarkOfWrite = lowWaterMarkOfWrite;
      return this;
    }

    public Builder basePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder cleanupThreshold(double cleanupThreshold) {
      this.cleanupThreshold = cleanupThreshold;
      return this;
    }

    public Builder highWaterMarkOfWrite(double highWaterMarkOfWrite) {
      this.highWaterMarkOfWrite = highWaterMarkOfWrite;
      return this;
    }

    public Builder cleanIntervalMs(long cleanIntervalMs) {
      this.cleanIntervalMs = cleanIntervalMs;
      return this;
    }

    public DiskItem build() {
      return new DiskItem(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @VisibleForTesting
  DiskMetaData getDiskMetaData() {
    return diskMetaData;
  }

  public void removeResources(String shuffleKey) {
    diskMetaData.updateDiskSize(-diskMetaData.getShuffleSize(shuffleKey));
    diskMetaData.remoteShuffle(shuffleKey);
  }
}
