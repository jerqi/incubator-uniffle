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

import static java.util.Objects.requireNonNull;

public class ShuffleEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleEngine.class);

  private final String appId;
  private final String shuffleId;
  private final int startPartition;
  private final int endPartition;
  private final ShuffleServerConf conf;
  private final BufferManager bufferManager;
  private final String serverId;

  private ShuffleBuffer buffer;
  private boolean isCommit = false;
  private long timestamp;

  public ShuffleEngine(
    String appId,
    String shuffleId,
    int startPartition,
    int endPartition,
    ShuffleServerConf conf,
    BufferManager bufferManager,
    String serverId) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;

    requireNonNull(conf);
    requireNonNull(bufferManager);
    this.conf = conf;
    this.bufferManager = bufferManager;
    this.serverId = serverId;
  }

  public ShuffleEngine(
    String appId,
    String shuffleId,
    int startPartition,
    int endPartition) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.conf = null;
    this.bufferManager = null;
    this.serverId = "";
  }

  public synchronized StatusCode init() {
    buffer = bufferManager.getBuffer(startPartition, endPartition);
    if (buffer == null) {
      return StatusCode.NO_BUFFER;
    }

    ShuffleServerMetrics.decAvailableBuffer(1);
    updateTimestamp();

    return StatusCode.SUCCESS;
  }

  public synchronized StatusCode write(
    List<ShufflePartitionedData> shuffleData) throws IOException, IllegalStateException {
    if (buffer == null) {
      // is committed
      buffer = bufferManager.getBuffer(startPartition, endPartition);

      if (buffer == null) {
        return StatusCode.NO_BUFFER;
      }
      isCommit = false;
    }

    for (ShufflePartitionedData data : shuffleData) {
      StatusCode ret = write(data);
      if (ret != StatusCode.SUCCESS) {
        return ret;
      }
    }

    updateTimestamp();
    return StatusCode.SUCCESS;
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

  public synchronized void flush() throws IOException, IllegalStateException {
    ShuffleStorageWriteHandler writeHandler = getWriteHandler();
    for (int partition = startPartition; partition <= endPartition; ++partition) {
      writeHandler.write(buffer.getBlocks(partition));
    }

    ShuffleServerMetrics.incBlockWriteSize(buffer.getSize());
    ShuffleServerMetrics.incBlockWriteNum(buffer.getBlockNum());
    ShuffleServerMetrics.incBlockWriteNum(buffer.getBlockNum());
    buffer.clear();
  }

  public synchronized void commit() throws IOException, IllegalStateException {
    if (!isCommit) {
      bufferManager.getAtomicCount().decrementAndGet();
      isCommit = true;
    }

    if (buffer != null) {
      if (buffer.getSize() > 0) {
        flush();
      }
      buffer = null;
    }
    updateTimestamp();
  }

  public synchronized void reclaim() {
    if (!isCommit) {
      LOGGER.warn("{} {} {}~{} is not commit yet!", appId, shuffleId, startPartition, endPartition);
      isCommit = true;
      bufferManager.getAtomicCount().decrementAndGet();
    }

    if (buffer != null) {
      buffer.clear();
    }
    buffer = null;
  }

  private ShuffleStorageWriteHandler getWriteHandler() throws IOException, IllegalStateException {
    StorageType storageType = StorageType.valueOf(conf.getString(ShuffleServerConf.DATA_STORAGE_TYPE));


    if (storageType == StorageType.FILE) {
      return new FileBasedShuffleWriteHandler(getBasePath(), serverId, getHadoopConf());
    } else {
      String msg = "Unsupported storage type: " + storageType;
      LOGGER.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  @VisibleForTesting
  long getTimestamp() {
    return this.timestamp;
  }

  @VisibleForTesting
  void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @VisibleForTesting
  ShuffleBuffer getBuffer() {
    return this.buffer;
  }

  @VisibleForTesting
  Boolean getIsCommit() {
    return this.isCommit;
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

  @VisibleForTesting
  BufferManager getBufferManager() {
    return this.bufferManager;
  }

  public int getStartPartition() {
    return this.startPartition;
  }

  public int getEndPartition() {
    return this.endPartition;
  }

  private void updateTimestamp() {
    this.timestamp = System.currentTimeMillis();
  }

  private Configuration getHadoopConf() {
    return new Configuration();
  }

}
