package com.tencent.rss.server;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import com.tencent.rss.storage.ShuffleStorageWriteHandler;
import com.tencent.rss.storage.StorageType;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleEngine {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleEngine.class);
  private static final Logger LOG_RSS_INFO = LoggerFactory.getLogger(Constants.LOG4J_RSS_SHUFFLE_PREFIX);
  private static AtomicLong ATOMIC_EVENT_ID = new AtomicLong(0);

  private final String appId;
  private final String shuffleId;
  private final int startPartition;
  private final int endPartition;
  private final ShuffleServerConf conf;
  private final BufferManager bufferManager;
  private final ShuffleFlushManager shuffleFlushManager;
  private final String serverId;

  private ShuffleBuffer buffer;
  private long timestamp;
  private Set<Long> eventIds = Sets.newConcurrentHashSet();

  public ShuffleEngine(
      String appId,
      String shuffleId,
      int startPartition,
      int endPartition,
      ShuffleServerConf conf,
      BufferManager bufferManager,
      ShuffleFlushManager shuffleFlushManager,
      String serverId) {
    requireNonNull(conf);
    requireNonNull(bufferManager);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.conf = conf;
    this.bufferManager = bufferManager;
    this.shuffleFlushManager = shuffleFlushManager;
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
    this.shuffleFlushManager = null;
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

  public synchronized StatusCode write(ShufflePartitionedData data) throws IOException, IllegalStateException {
    if (buffer == null) {
      buffer = bufferManager.getBuffer(startPartition, endPartition);

      if (buffer == null) {
        return StatusCode.NO_BUFFER;
      }
    }

    StatusCode ret = buffer.append(data);
    if (ret != StatusCode.SUCCESS) {
      return ret;
    }
    LOG_RSS_INFO.info("Add blockIds for " + getAppInfo() + " successfully "
        + getBlockIds(data.getBlockList()));

    if (buffer.full()) {
      LOG_RSS_INFO.info("Buffer is full after cache " + data + ", prepare to flush.");
      asynFlush();
    }

    return StatusCode.SUCCESS;
  }

  // add blocks to queue, they will be flushed to storage asynchronous
  public synchronized void asynFlush() throws IllegalStateException {
    for (int partition = startPartition; partition <= endPartition; ++partition) {
      // buffer will be cleared, and new list must be created for asyn flush
      List<ShufflePartitionedBlock> spbs = Lists.newArrayList(buffer.getBlocks(partition));
      ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(ATOMIC_EVENT_ID.getAndIncrement(),
          appId, shuffleId, startPartition, endPartition, spbs);
      shuffleFlushManager.addToFlushQueue(event);
      eventIds.add(event.getEventId());
      LOG_RSS_INFO.info("Add event to flush queue for " + getAppInfo() + " successfully " + event);
    }
    buffer.clear();
  }

  private String getAppInfo() {
    return "appId[" + appId + "], shuffleId[" + shuffleId
        + "], partition[" + startPartition + "-" + endPartition + "]";
  }

  private List<Long> getBlockIds(List<ShufflePartitionedBlock> spbs) {
    List<Long> blockIds = Lists.newArrayList();
    for (ShufflePartitionedBlock spb : spbs) {
      blockIds.add(spb.getBlockId());
    }
    return blockIds;
  }

  public synchronized Set<Long> commit() throws IOException, IllegalStateException {
    if (buffer == null) {
      buffer = bufferManager.getBuffer(startPartition, endPartition);
    }
    bufferManager.getAtomicCount().decrementAndGet();
    asynFlush();
    buffer = null;
    updateTimestamp();
    Set<Long> snapshot = Sets.newHashSet(eventIds);
    eventIds.clear();
    return snapshot;
  }

  public synchronized void reclaim() {
    if (buffer != null) {
      LOG.warn("{} {} {}~{} is not commit yet!", appId, shuffleId, startPartition, endPartition);
      bufferManager.getAtomicCount().decrementAndGet();
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
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  @VisibleForTesting
  long getTimestamp() {
    return timestamp;
  }

  @VisibleForTesting
  void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @VisibleForTesting
  ShuffleBuffer getBuffer() {
    return buffer;
  }

  @VisibleForTesting
  String getBasePath() {
    String basePath = conf.getString(ShuffleServerConf.DATA_STORAGE_BASE_PATH);
    String subPath = RssUtils.getShuffleDataPath(appId, String.valueOf(shuffleId), startPartition, endPartition);
    return RssUtils.getFullShuffleDataFolder(basePath, subPath);
  }

  @VisibleForTesting
  BufferManager getBufferManager() {
    return bufferManager;
  }

  private void updateTimestamp() {
    this.timestamp = System.currentTimeMillis();
  }

  private Configuration getHadoopConf() {
    return new Configuration();
  }

  public int getStartPartition() {
    return startPartition;
  }

  public int getEndPartition() {
    return endPartition;
  }
}
