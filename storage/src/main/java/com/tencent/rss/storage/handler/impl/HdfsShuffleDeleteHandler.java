package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsShuffleDeleteHandler implements ShuffleDeleteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsShuffleDeleteHandler.class);

  private Configuration hadoopConf;

  public HdfsShuffleDeleteHandler(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  @Override
  public void delete(String[] storageBasePaths, String appId) {
    Path path = new Path(ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePaths[0], appId));
    boolean isSuccess = false;
    int times = 0;
    int retryMax = 5;
    long start = System.currentTimeMillis();
    LOG.info("Try delete shuffle data in HDFS for appId[" + appId + "] with " + path);
    while (!isSuccess && times < retryMax) {
      try {
        FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);
        fileSystem.delete(path, true);
        isSuccess = true;
      } catch (Exception e) {
        times++;
        LOG.warn("Can't delete shuffle data for appId[" + appId + "] with " + times + " times", e);
        try {
          Thread.sleep(1000);
        } catch (Exception ex) {
          LOG.warn("Exception happened when Thread.sleep", ex);
        }
      }
    }
    if (isSuccess) {
      LOG.info("Delete shuffle data in HDFS for appId[" + appId + "] with " + path + " successfully in "
          + (System.currentTimeMillis() - start) + " ms");
    } else {
      LOG.info("Failed to delete shuffle data in HDFS for appId[" + appId + "] with " + path + " in "
          + (System.currentTimeMillis() - start) + " ms");
    }
  }
}
