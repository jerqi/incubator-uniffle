package com.tencent.rss.storage.common;

public class DiskItem {

  private final long capacity;
  private final String basePath;
  private final double cleanupThreshold;
  private final double highWaterMarkOfWrite;
  private final double lowWaterMarkOfWrite;
  private DiskMetaData directoryMetaData;

  public DiskItem(long capacity, String basePath, double cleanupThreshold, double highWaterMarkOfWrite,
      double lowWaterMarkOfWrite) {
    this.capacity = capacity;
    this.basePath = basePath;
    this.cleanupThreshold = cleanupThreshold;
    this.highWaterMarkOfWrite = highWaterMarkOfWrite;
    this.lowWaterMarkOfWrite = lowWaterMarkOfWrite;
    initialize();
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

  private void clean() {

  }

  private void upload() {

  }

}
