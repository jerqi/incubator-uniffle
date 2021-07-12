package com.tencent.rss.storage.handler.api;

import com.tencent.rss.storage.util.ShuffleUploadResult;
import java.io.File;
import java.util.List;

public interface ShuffleUploadHandler {

  /**
   * Upload data files and index files to remote storage, the correctness of relation between
   * items of data files, index files and partitions is guaranteed by caller/user.
   * Normally we best-effort strategy to upload files, so the result may be part of the files.
   *
   * @param dataFiles   local data files to be uploaded to remote storage
   * @param indexFiles  local index files to be uploaded to remote storage
   * @param partitions  partition id of the local data files and index files
   *
   * @return  upload result including total successful uploaded file size and partition id list
   *
   */
  ShuffleUploadResult upload(List<File> dataFiles, List<File> indexFiles, List<Integer> partitions);

}
