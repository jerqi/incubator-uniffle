package com.tencent.rss.storage.handler.api;

public interface ShuffleDeleteHandler {

  /**
   * Delete shuffle data with appId
   *
   * @param appId ApplicationId for delete
   */
  void delete(String[] storageBasePaths, String appId);
}
