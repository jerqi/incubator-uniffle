package com.tencent.rss.server;

import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import com.tencent.rss.storage.ShuffleStorageWriteHandler;
import com.tencent.rss.storage.StorageType;
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
  private StorageType storageType;

  public ShuffleEngine(String appId, String shuffleId, int startPartition, int endPartition) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
  }

  public StatusCode init() throws IOException, IllegalStateException {
    synchronized (this) {
      buffer = BufferManager.instance().getBuffer(startPartition, endPartition);
      if (buffer == null) {
        return StatusCode.NO_BUFFER;
      }

      storageType = ShuffleTaskManager.instance().storageType;
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

  public StatusCode flush() throws IOException, IllegalStateException {
    synchronized (this) {
      ShuffleStorageWriteHandler writeHandler = getWriteHandler();

      for (int partition = startPartition; partition <= endPartition; ++partition) {
        writeHandler.write(buffer.getBlocks(partition));
      }

      ShuffleServerMetrics.incBlockWriteSize(buffer.getSize());
      ShuffleServerMetrics.incBlockWriteNum(buffer.getBlockNum());
      ShuffleServerMetrics.incBlockWriteNum(buffer.getBlockNum());

      buffer.clear();

      return StatusCode.SUCCESS;
    }
  }

  private ShuffleStorageWriteHandler getWriteHandler() throws IOException, IllegalStateException {
    if (storageType == StorageType.FILE) {
      return new FileBasedShuffleWriteHandler("", "", null);
    } else {
      String msg = "Unsupported storage type: " + storageType;
      LOGGER.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  private void clear() {
    if (buffer != null) {
      buffer.clear();
    }
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getShuffleId() {
    return shuffleId;
  }

  public void setShuffleId(String shuffleId) {
    this.shuffleId = shuffleId;
  }

  public int getStartPartition() {
    return startPartition;
  }

  public void setStartPartition(int startPartition) {
    this.startPartition = startPartition;
  }

  public int getEndPartition() {
    return endPartition;
  }

  public void setEndPartition(int endPartition) {
    this.endPartition = endPartition;
  }

}
