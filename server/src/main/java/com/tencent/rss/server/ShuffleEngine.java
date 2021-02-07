package com.tencent.rss.server;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.RssUtils;
import java.io.IOException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleEngine {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleEngine.class);
  private final String appId;
  private final int shuffleId;
  private final int startPartition;
  private final int endPartition;
  private final ShuffleServerConf conf;
  private final BufferManager bufferManager;

  private ShuffleBuffer buffer;
  private long timestamp;

  private Set<Long> eventIds = Sets.newConcurrentHashSet();

  public ShuffleEngine(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      ShuffleServerConf conf,
      BufferManager bufferManager) {
    requireNonNull(conf);
    requireNonNull(bufferManager);
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.conf = conf;
    this.bufferManager = bufferManager;
  }

  public ShuffleEngine(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.conf = null;
    this.bufferManager = null;
  }

  public synchronized StatusCode init() {
    buffer = bufferManager.getBuffer(this);
    if (buffer == null) {
      return StatusCode.NO_BUFFER;
    }

    updateTimestamp();

    return StatusCode.SUCCESS;
  }

  public synchronized StatusCode write(ShufflePartitionedData data) throws IOException, IllegalStateException {
    if (buffer == null) {
      // TODO: should not be here, buffer is null only after gc but engine would be null then.
      String engineDesc =
          String.join("-", appId, String.valueOf(shuffleId),
              String.valueOf(startPartition), String.valueOf(endPartition));
      String msg = "Buffer of engine: "
          + engineDesc
          + " is null, which should only happen after gc but engine would be null too.";
      throw new IllegalStateException(msg);
    }

    if (bufferManager.isFull()) {
      return StatusCode.NO_BUFFER;
    }

    StatusCode ret = buffer.append(data);

    if (ret != StatusCode.SUCCESS) {
      return ret;
    }

    return StatusCode.SUCCESS;
  }

  public synchronized Set<Long> commit() throws IllegalStateException {
    if (buffer == null) {
      // TODO: should not be here, buffer is null only after gc but engine would be null then.
      String engineDesc =
          String.join("-", appId, String.valueOf(shuffleId),
              String.valueOf(startPartition), String.valueOf(endPartition));
      String msg = "Buffer of engine: "
          + engineDesc
          + " is null, which should only happen after gc but engine would be null too.";
      throw new IllegalStateException(msg);
    }

    buffer.flush();
    Set<Long> snapshot = Sets.newHashSet(eventIds);
    eventIds.clear();
    updateTimestamp();

    return snapshot;
  }

  public void reclaim() {
    buffer = null;
  }

  public String makeKey() {
    return String.join(
        "~",
        appId,
        String.valueOf(shuffleId),
        String.valueOf(startPartition),
        String.valueOf(endPartition));
  }

  public Set<Long> getEventIds() {
    return eventIds;
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
    String subPath = RssUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition);
    return RssUtils.getFullShuffleDataFolder(basePath, subPath);
  }

  @VisibleForTesting
  BufferManager getBufferManager() {
    return bufferManager;
  }

  private void updateTimestamp() {
    this.timestamp = System.currentTimeMillis();
  }

  public String getAppId() {
    return appId;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  public int getStartPartition() {
    return startPartition;
  }

  public int getEndPartition() {
    return endPartition;
  }
}
