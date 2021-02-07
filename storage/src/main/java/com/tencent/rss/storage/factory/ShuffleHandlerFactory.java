package com.tencent.rss.storage.factory;

import com.tencent.rss.storage.handler.api.ShuffleReadHandler;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleReadHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;
import com.tencent.rss.storage.utils.StorageType;

public class ShuffleHandlerFactory {

  private static ShuffleHandlerFactory INSTANCE;

  private ShuffleHandlerFactory() {
  }

  public static synchronized ShuffleHandlerFactory getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new ShuffleHandlerFactory();
    }
    return INSTANCE;
  }

  public ShuffleReadHandler createShuffleReadHandler(CreateShuffleReadHandlerRequest request) {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new HdfsShuffleReadHandler(request.getAppId(), request.getShuffleId(), request.getPartitionId(),
          request.getIndexReadLimit(), request.getPartitionsPerServer(), request.getPartitionNum(),
          request.getReadBufferSize(), request.getStorageBasePath(), request.getExpectedBlockIds());
    } else {
      throw new UnsupportedOperationException("Doesn't support storage type:" + request.getStorageType());
    }
  }

  public ShuffleWriteHandler createShuffleWriteHandler(CreateShuffleWriteHandlerRequest request) throws Exception {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new HdfsShuffleWriteHandler(request.getAppId(), request.getShuffleId(), request.getStartPartition(),
          request.getEndPartition(), request.getStorageBasePath(), request.getFileNamePrefix(), request.getConf());
    } else {
      throw new UnsupportedOperationException("Doesn't support storage type:" + request.getStorageType());
    }
  }
}
