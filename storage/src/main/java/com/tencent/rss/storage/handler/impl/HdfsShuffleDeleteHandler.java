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
    try {
      FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);
      fileSystem.delete(path, true);
    } catch (Exception e) {
      LOG.warn("Can't delete shuffle data for appId[" + appId + "]", e);
    }
  }
}
