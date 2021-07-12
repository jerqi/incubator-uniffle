package com.tencent.rss.storage.factory;

import com.tencent.rss.storage.handler.api.ShuffleUploadHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleUploadHandler;
import com.tencent.rss.storage.request.CreateShuffleUploadHandlerRequest;
import com.tencent.rss.storage.util.StorageType;
import java.io.IOException;

public class ShuffleUploadHandlerFactory {

  public ShuffleUploadHandlerFactory() {

  }

  private static class LazyHolder {
    static final ShuffleUploadHandlerFactory INSTANCE = new ShuffleUploadHandlerFactory();
  }

  public static ShuffleUploadHandlerFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  public ShuffleUploadHandler createShuffleUploadHandler(
      CreateShuffleUploadHandlerRequest request) throws IOException, RuntimeException {
    if (request.getRemoteStorageType() == StorageType.HDFS) {
      return new HdfsShuffleUploadHandler(
          request.getRemoteStorageBasePath(),
          request.getHadoopConf(),
          request.getHdfsFilePrefix(),
          request.getBufferSize(),
          request.getCombineUpload());
    } else {
      throw new RuntimeException("Unsupported remote storage type " + request.getRemoteStorageType().name());
    }
  }
}
