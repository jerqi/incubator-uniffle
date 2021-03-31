package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileDeleteHandler implements ShuffleDeleteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileDeleteHandler.class);

  @Override
  public void delete(String[] storageBasePaths, String appId) {
    for (String basePath : storageBasePaths) {
      String shufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(basePath, appId);
      LOG.info("Delete shuffle data for appId[" + appId + "] with " + shufflePath);
      try {
        File baseFolder = new File(shufflePath);
        FileUtils.deleteDirectory(baseFolder);
      } catch (Exception e) {
        LOG.warn("Can't delete shuffle data for appId[" + appId + "] with " + shufflePath, e);
      }
    }
  }
}
