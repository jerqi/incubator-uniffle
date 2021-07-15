package com.tencent.rss.storage.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class DiskItem {

  private static final Logger LOG = LoggerFactory.getLogger(DiskMetaData.class);

  private final long capacity;
  private final String basePath;
  private final double cleanupThreshold;
  private final double highWaterMarkOfWrite;
  private final double lowWaterMarkOfWrite;
  private DiskMetaData diskMetaData = new DiskMetaData();

  private final Runnable cleaner = () -> {

  };

  public DiskItem(long capacity, String basePath, double cleanupThreshold, double highWaterMarkOfWrite,
      double lowWaterMarkOfWrite) {

    this.capacity = capacity;
    this.basePath = basePath;
    this.cleanupThreshold = cleanupThreshold;
    this.highWaterMarkOfWrite = highWaterMarkOfWrite;
    this.lowWaterMarkOfWrite = lowWaterMarkOfWrite;
    initialize();
    // todo: extract a class named Service, and support stop method. Now
    // we assume that it's enough for one thread per disk. If in testing mode
    // the thread won't be started.
    new Thread(basePath + "-Cleaner") {
      @Override
      public void run() {
        setDaemon(true);
        for (;;) {
          try {
              clean();
              // todo: get sleepInterval from configuration
              Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
          } catch (Exception e) {
            LOG.error(getName() + " happened exception: ", e);
          }
        }
      }
    }.start();
  }

  void initialize() throws RuntimeException {
    // create the base path is not exist and throw runtime exception if fail.

  }

  public boolean canWrite() {
    // TODO: start force clean signal to cleaner and uploader
    return true;
  }

  public String getBasePath() {
    return basePath;
  }

  public void updateWrite(String key, long delta) {
    // TODO: update metadata and send signal to cleaner
  }

  public void updateRead(String key, long delta) {
    // TODO: update metadata and send signal to uploader
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
          diskMetaData.updateDiskSize(-diskMetaData.getShuffleSize(shuffleKey));
          diskMetaData.remoteShuffle(shuffleKey);
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
}
