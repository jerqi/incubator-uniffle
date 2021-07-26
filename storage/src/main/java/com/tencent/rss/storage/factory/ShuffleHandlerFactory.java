package com.tencent.rss.storage.factory;

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.factory.ShuffleServerClientFactory;
import com.tencent.rss.client.util.ClientType;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.storage.handler.api.ClientReadHandler;
import com.tencent.rss.storage.handler.api.ServerReadHandler;
import com.tencent.rss.storage.handler.api.ShuffleDeleteHandler;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.handler.impl.MultiStorageHdfsClientReadHandler;
import com.tencent.rss.storage.handler.impl.HdfsClientReadHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleDeleteHandler;
import com.tencent.rss.storage.handler.impl.HdfsShuffleWriteHandler;
import com.tencent.rss.storage.handler.impl.LocalFileClientReadHandler;
import com.tencent.rss.storage.handler.impl.LocalFileDeleteHandler;
import com.tencent.rss.storage.handler.impl.LocalFileServerReadHandler;
import com.tencent.rss.storage.handler.impl.LocalFileWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleDeleteHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;
import com.tencent.rss.storage.util.StorageType;
import java.util.List;
import java.util.stream.Collectors;

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

  public ClientReadHandler createShuffleReadHandler(CreateShuffleReadHandlerRequest request) {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new HdfsClientReadHandler(
          request.getAppId(),
          request.getShuffleId(),
          request.getPartitionId(),
          request.getIndexReadLimit(),
          request.getPartitionNumPerRange(),
          request.getPartitionNum(),
          request.getReadBufferSize(),
          request.getStorageBasePath(),
          request.getHadoopConf());
    } else if (StorageType.LOCALFILE.name().equals(request.getStorageType())) {
      List<ShuffleServerInfo> shuffleServerInfoList = request.getShuffleServerInfoList();
      List<ShuffleServerClient> shuffleServerClients = shuffleServerInfoList.stream().map(
          ssi -> ShuffleServerClientFactory.getInstance().getShuffleServerClient(ClientType.GRPC.name(), ssi)).collect(
          Collectors.toList());
      return new LocalFileClientReadHandler(request.getAppId(), request.getShuffleId(), request.getPartitionId(),
          request.getIndexReadLimit(), request.getPartitionNumPerRange(), request.getPartitionNum(),
          request.getReadBufferSize(), shuffleServerClients);
    } else  if ("HDFS_BACKUP".equals(request.getStorageType())) {
      return new MultiStorageHdfsClientReadHandler(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionId(),
        request.getIndexReadLimit(),
        request.getPartitionNumPerRange(),
        request.getPartitionNum(),
        request.getReadBufferSize(),
        request.getStorageBasePath(),
        request.getExpectedBlockIds(),
        request.getHadoopConf());
    } else {
      throw new UnsupportedOperationException(
          "Doesn't support storage type for client read handler:" + request.getStorageType());
    }
  }

  public ServerReadHandler createServerReadHandler(CreateShuffleReadHandlerRequest request) {
    if (StorageType.LOCALFILE.name().equals(request.getStorageType())) {
      return new LocalFileServerReadHandler(request.getAppId(), request.getShuffleId(),
          request.getPartitionId(), request.getPartitionNumPerRange(), request.getPartitionNum(),
          request.getReadBufferSize(), request.getRssBaseConf());
    } else {
      throw new UnsupportedOperationException(
          "Doesn't support storage type for server read handler:" + request.getStorageType());
    }
  }

  public ShuffleWriteHandler createShuffleWriteHandler(CreateShuffleWriteHandlerRequest request) throws Exception {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new HdfsShuffleWriteHandler(request.getAppId(), request.getShuffleId(),
          request.getStartPartition(), request.getEndPartition(), request.getStorageBasePaths()[0],
          request.getFileNamePrefix(), request.getConf());
    } else if (StorageType.LOCALFILE.name().equals(request.getStorageType())) {
      return new LocalFileWriteHandler(request.getAppId(), request.getShuffleId(), request.getStartPartition(),
          request.getEndPartition(), request.getStorageBasePaths(), request.getFileNamePrefix());
    } else {
      throw new UnsupportedOperationException("Doesn't support storage type for shuffle write handler:"
          + request.getStorageType());
    }
  }

  public ShuffleDeleteHandler createShuffleDeleteHandler(CreateShuffleDeleteHandlerRequest request) {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new HdfsShuffleDeleteHandler(request.getConf());
    } else if (StorageType.LOCALFILE.name().equals(request.getStorageType())) {
      return new LocalFileDeleteHandler();
    } else {
      throw new UnsupportedOperationException("Doesn't support storage type for shuffle delete handler:"
          + request.getStorageType());
    }
  }
}
