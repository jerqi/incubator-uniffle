package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ClientReadHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import com.tencent.rss.storage.util.StorageType;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.IOException;

public class MultiStorageReadHandler extends AbstractFileClientReadHandler {
  private ClientReadHandler clientReadHandler;
  private CreateShuffleReadHandlerRequest fallbackRequest;
  private final Roaring64NavigableMap expectBlockIds;
  private final Roaring64NavigableMap processBlockIds;
  private int offsetIndex;

  public MultiStorageReadHandler(
      StorageType primary,
      StorageType secondary,
      CreateShuffleReadHandlerRequest request,
      Roaring64NavigableMap expectBlockIds,
      Roaring64NavigableMap processBlockIds) {
    request.setStorageType(primary.name());
    this.clientReadHandler = ShuffleHandlerFactory.getInstance().createShuffleReadHandler(request);
    request.setStorageType(secondary.name());
    this.fallbackRequest = request;
    this.expectBlockIds = expectBlockIds;
    this.processBlockIds = processBlockIds;
    this.offsetIndex = 0;
  }

  @Override
  public ShuffleDataResult readShuffleData(int segmentIndex) {
    ShuffleDataResult result = clientReadHandler.readShuffleData(segmentIndex - offsetIndex);
    if (result != null && !result.isEmpty()) {
      return result;
    } else {
      if (fallbackRequest != null && !checkBlocks()) {
        clientReadHandler.close();
        clientReadHandler = createShuffleRemoteStorageReadHandler(fallbackRequest);
        offsetIndex = segmentIndex;
        fallbackRequest = null;
        result = clientReadHandler.readShuffleData(0);
        if (result != null && !result.isEmpty()) {
          return result;
        }
      }
    }
    return null;
  }

  @Override
  public void close() {
    clientReadHandler.close();
  }

  private boolean checkBlocks() {
    Roaring64NavigableMap cloneBitmap = cloneBitmap(expectBlockIds);
    cloneBitmap.and(processBlockIds);
    return cloneBitmap.equals(expectBlockIds);
  }

  private Roaring64NavigableMap cloneBitmap(Roaring64NavigableMap bitmap) {
    Roaring64NavigableMap cloneBitmap;
    try {
      cloneBitmap = RssUtils.deserializeBitMap(RssUtils.serializeBitMap(bitmap));
    } catch (IOException ioe) {
      throw new RuntimeException("clone bitmap exception", ioe);
    }
    return cloneBitmap;
  }

  private ClientReadHandler createShuffleRemoteStorageReadHandler(CreateShuffleReadHandlerRequest request) {
    if (StorageType.HDFS.name().equals(request.getStorageType())) {
      return new MultiStorageHdfsClientReadHandler(
          request.getAppId(),
          request.getShuffleId(),
          request.getPartitionId(),
          request.getIndexReadLimit(),
          request.getPartitionNumPerRange(),
          request.getPartitionNum(),
          request.getReadBufferSize(),
          request.getStorageBasePath(),
          request.getHadoopConf());
    } else {
      throw new UnsupportedOperationException(
          "Doesn't support storage type for client remote storage read handler:" + request.getStorageType());
    }
  }
}
