package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import com.tencent.rss.storage.ShuffleStorageWriteHandler;
import com.tencent.rss.storage.StorageType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ShuffleEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleEngine.class);

  private String appId;
  private String shuffleId;
  private int startPartition;
  private int endPartition;
  private ShuffleBuffer buffer;
  private ShuffleServerConf conf;

  public ShuffleEngine(String appId, String shuffleId, int startPartition, int endPartition, ShuffleServerConf conf) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.conf = conf;
  }

  public ShuffleEngine(String appId, String shuffleId, int startPartition, int endPartition) {
    this(appId, shuffleId, startPartition, endPartition, null);
  }

  public StatusCode init() {
    synchronized (this) {
      buffer = BufferManager.instance().getBuffer(startPartition, endPartition);
      if (buffer == null) {
        return StatusCode.NO_BUFFER;
      }

      ShuffleServerMetrics.decAvailableBuffer(1);

      return StatusCode.SUCCESS;
    }
  }

  public StatusCode write(List<ShufflePartitionedData> shuffleData) throws IOException, IllegalStateException {
    synchronized (this) {
      if (buffer == null) {
        // is committed
        buffer = BufferManager.instance().getBuffer(startPartition, endPartition);

        if (buffer == null) {
          return StatusCode.NO_BUFFER;
        }
      }

      for (ShufflePartitionedData data : shuffleData) {
        StatusCode ret = write(data);
        if (ret != StatusCode.SUCCESS) {
          return ret;
        }
      }

      return StatusCode.SUCCESS;
    }
  }

  private StatusCode write(ShufflePartitionedData data) throws IOException, IllegalStateException {
    StatusCode ret = buffer.append(data);
    if (ret != StatusCode.SUCCESS) {
      return ret;
    }

    if (buffer.full()) {
      flush();
    }

    return StatusCode.SUCCESS;
  }

  public void flush() throws IOException, IllegalStateException {
    synchronized (this) {
      ShuffleStorageWriteHandler writeHandler = getWriteHandler();

      for (int partition = startPartition; partition <= endPartition; ++partition) {
        writeHandler.write(buffer.getBlocks(partition));
      }

      ShuffleServerMetrics.incBlockWriteSize(buffer.getSize());
      ShuffleServerMetrics.incBlockWriteNum(buffer.getBlockNum());
      ShuffleServerMetrics.incBlockWriteNum(buffer.getBlockNum());
      buffer.clear();
    }
  }

  private ShuffleStorageWriteHandler getWriteHandler() throws IOException, IllegalStateException {
    StorageType storageType = StorageType.valueOf(conf.getString(ShuffleServerConf.DATA_STORAGE_TYPE));


    if (storageType == StorageType.FILE) {
      return new FileBasedShuffleWriteHandler(getBasePath(), ShuffleServer.id, getHadoopConf());
    } else {
      String msg = "Unsupported storage type: " + storageType;
      LOGGER.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  @VisibleForTesting
  ShuffleBuffer getBuffer() {
    return this.buffer;
  }

  @VisibleForTesting
  String getBasePath() {
    String basePath = conf.getString(ShuffleServerConf.DATA_STORAGE_BASE_PATH);
    String subPath = String.join(
      "_",
      appId,
      shuffleId,
      String.join("-", String.valueOf(startPartition), String.valueOf(endPartition)));
    return String.join("/", basePath, subPath);
  }

  private Configuration getHadoopConf() {
    return new Configuration();
  }

}
