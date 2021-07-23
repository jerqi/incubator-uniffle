package com.tencent.rss.storage.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
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

  // todo: refactor this construct method by builder pattern
  public DiskItem(
      String basePath,
      double cleanupThreshold,
      double highWaterMarkOfWrite,
      double lowWaterMarkOfWrite,
      long capacity,
      long cleanIntervalMs) {

    this.basePath = basePath;
    this.cleanupThreshold = cleanupThreshold;
    this.highWaterMarkOfWrite = highWaterMarkOfWrite;
    this.lowWaterMarkOfWrite = lowWaterMarkOfWrite;
    this.capacity = capacity;
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
    new Thread(basePath + "-Cleaner") {
      @Override
      public void run() {
        setDaemon(true);
        for (;;) {
          try {
              clean();
              // todo: get sleepInterval from configuration
              Uninterruptibles.sleepUninterruptibly(cleanIntervalMs, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
            LOG.error(getName() + " happened exception: ", e);
          }
        }
      }
    }.start();
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
    diskMetaData.updateDiskSize(delta);
    diskMetaData.updateShuffleSize(shuffleKey, delta);
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
      // uploaded totally, `uploadSize` will equal `Size`
      if (diskMetaData.getShuffleHasRead(shuffleKey)
          && diskMetaData.getShuffleSize(shuffleKey) == diskMetaData.getShuffleUploadedSize(shuffleKey)) {
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

  private void upload() {

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
