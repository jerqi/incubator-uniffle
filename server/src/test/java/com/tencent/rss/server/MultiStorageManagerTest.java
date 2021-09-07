package com.tencent.rss.server;

import com.tencent.rss.common.config.RssBaseConf;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MultiStorageManagerTest {

  @Test
  public void ConstructorTest() throws IOException {
    ShuffleServerConf conf = new ShuffleServerConf();
    boolean isException = false;
    try {
        new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("Base path dirs must not be empty"));
    }
    assertTrue(isException);
    isException = false;
    TemporaryFolder tmp = new TemporaryFolder();
    tmp.create();
    conf.set(RssBaseConf.RSS_STORAGE_BASE_PATH, tmp.getRoot().getAbsolutePath());
    conf.set(ShuffleServerConf.RSS_DISK_CAPACITY, -1L);
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("Capacity must be larger than zero"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_DISK_CAPACITY, 100L);
    conf.set(ShuffleServerConf.RSS_CLEANUP_THRESHOLD, -1.0);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("cleanupThreshold must be between 0 and 100"));
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.RSS_CLEANUP_THRESHOLD, 999.9);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("cleanupThreshold must be between 0 and 100"));
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.RSS_CLEANUP_THRESHOLD, 85.1);
    conf.set(ShuffleServerConf.RSS_HIGH_WATER_MARK_OF_WRITE, 10.0);
    conf.set(ShuffleServerConf.RSS_LOW_WATER_MARK_OF_WRITE, 20.0);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("highWaterMarkOfWrite must be larger than lowWaterMarkOfWrite"));
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.RSS_HIGH_WATER_MARK_OF_WRITE, 120.0);
    conf.set(ShuffleServerConf.RSS_LOW_WATER_MARK_OF_WRITE, -10.0);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("lowWaterMarkOfWrite must be larger than zero"));
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.RSS_HIGH_WATER_MARK_OF_WRITE, 120.0);
    conf.set(ShuffleServerConf.RSS_LOW_WATER_MARK_OF_WRITE,  10.0);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("highWaterMarkOfWrite must be smaller than 100"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_HIGH_WATER_MARK_OF_WRITE, 90.0);
    conf.set(ShuffleServerConf.RSS_UPLOADER_THREAD_NUM, -1);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("uploadThreadNum must be larger than 0"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_UPLOADER_THREAD_NUM, 1);
    conf.set(ShuffleServerConf.RSS_UPLOADER_INTERVAL_MS, -1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("uploadIntervalMs must be larger than 0"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_UPLOADER_INTERVAL_MS, 1L);
    conf.set(ShuffleServerConf.RSS_UPLOAD_COMBINE_THRESHOLD_MB, -1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("uploadCombineThresholdMB must be larger than 0"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_UPLOAD_COMBINE_THRESHOLD_MB, 1L);
    conf.set(ShuffleServerConf.RSS_REFERENCE_UPLOAD_SPEED_MBS, -1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("referenceUploadSpeedMbps must be larger than 0"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_REFERENCE_UPLOAD_SPEED_MBS, 1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("hdfsBasePath couldn't be empty"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_HDFS_BASE_PATH, "testPath");
    conf.set(ShuffleServerConf.RSS_UPLOAD_STORAGE_TYPE, "LOCALFILE");
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("ploadRemoteStorageType couldn't be LOCALFILE or FILE"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_UPLOAD_STORAGE_TYPE, "XXXX");
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      isException = true;
      assertTrue(ie.getMessage().contains("No enum constant"));
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_UPLOAD_STORAGE_TYPE, "HDFS");

    conf.set(ShuffleServerConf.RSS_DISK_CAPACITY, 1024L * 1024L * 1024L * 1024 * 1024L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      assertTrue(ie.getMessage().contains("Disk Available Capacity"));
      isException = true;
    }
    assertTrue(isException);
    conf.set(ShuffleServerConf.RSS_DISK_CAPACITY, 100L);

    conf.set(ShuffleServerConf.RSS_CLEANUP_INTERVAL_MS, -1L);
    isException = false;
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      assertTrue(ie.getMessage().contains("cleanupInterval must be larger than zero"));
      isException = true;
    }
    assertTrue(isException);

    conf.set(ShuffleServerConf.RSS_CLEANUP_INTERVAL_MS, 100L);
    try {
      new MultiStorageManager(conf, "");
    } catch (IllegalArgumentException ie) {
      fail();
    }
  }
}