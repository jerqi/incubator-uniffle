package com.tencent.rss.client.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.common.BufferSegment;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleReadHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleReadClientImpl implements ShuffleReadClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleReadClientImpl.class);

  private int shuffleId;
  private int partitionId;
  private byte[] readBuffer;
  private Set<Long> expectedBlockIds;
  private Queue<Long> blockIdQueue = Queues.newLinkedBlockingQueue();
  private Set<Long> processedBlockIds = Sets.newHashSet();
  private Set<Long> remainBlockIds = Sets.newHashSet();
  private Map<Long, BufferSegment> processingBlockIds = Maps.newHashMap();
  private AtomicInteger readSegments = new AtomicInteger(0);
  private AtomicLong readDataTime = new AtomicLong(0);
  private AtomicLong readIndexTime = new AtomicLong(0);
  private AtomicLong checkBlockIdsTime = new AtomicLong(0);
  private ShuffleReadHandler shuffleReadHandler;

  public ShuffleReadClientImpl(
      String storageType,
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionsPerServer,
      int partitionNum,
      int readBufferSize,
      String storageBasePath,
      Set<Long> expectedBlockIds) {
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.expectedBlockIds = expectedBlockIds;
    this.remainBlockIds = Sets.newHashSet(expectedBlockIds);
    shuffleReadHandler =
        ShuffleHandlerFactory.getInstance().createShuffleReadHandler(
            new CreateShuffleReadHandlerRequest(storageType, appId, shuffleId, partitionId, indexReadLimit,
                partitionsPerServer, partitionNum, readBufferSize, storageBasePath, expectedBlockIds));
  }

  @Override
  public void checkExpectedBlockIds() {
    // there is no data
    if (expectedBlockIds == null || expectedBlockIds.isEmpty()) {
      LOG.info("There is no shuffle data for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]");
      return;
    }
    final long start = System.currentTimeMillis();
    verifyBlockIds();
    checkBlockIdsTime.addAndGet(System.currentTimeMillis() - start);
  }

  /**
   * Parse every FileBasedShuffleSegment, and check if have all required blockIds
   */
  private void verifyBlockIds() {
    Set<Long> actualBlockIds = shuffleReadHandler.getAllBlockIds();
    Set<Long> copy = Sets.newHashSet(expectedBlockIds);
    if (actualBlockIds.size() < expectedBlockIds.size()) {
      copy.removeAll(expectedBlockIds);
      throw new RuntimeException("Can't find blockIds " + copy + ", expected[" + expectedBlockIds + "]");
    }
  }

  @Override
  public byte[] readShuffleBlockData() {
    // all blocks are read, return
    if (remainBlockIds.isEmpty()) {
      return null;
    }
    if (processingBlockIds.isEmpty()) {
      readBuffer = shuffleReadHandler.readShuffleData(remainBlockIds);
      if (readBuffer == null) {
        return null;
      }
      processingBlockIds = shuffleReadHandler.getBlockIdToBufferSegment();
      blockIdQueue.addAll(processingBlockIds.keySet());
    }
    // get next blockId
    Long blockId = blockIdQueue.poll();
    if (blockId == null) {
      // shouldn't be here
      LOG.warn("There is no data in blockId queue, it shouldn't be here");
      return null;
    }
    BufferSegment bs = processingBlockIds.get(blockId);
    byte[] data = new byte[bs.getLength()];
    long expectedCrc = -1;
    long actualCrc = -1;
    try {
      System.arraycopy(readBuffer, bs.getOffset(), data, 0, bs.getLength());
      expectedCrc = bs.getCrc();
      actualCrc = ChecksumUtils.getCrc32(data);
    } catch (Exception e) {
      LOG.warn("Can't read data for blockId[" + blockId + "]", e);
    }
    if (expectedCrc != actualCrc) {
      throw new RuntimeException("Unexpected crc value for blockId[" + blockId
          + "], expected:" + expectedCrc + ", actual:" + actualCrc);
    }
    processingBlockIds.remove(blockId);
    processedBlockIds.add(blockId);
    remainBlockIds.remove(blockId);
    return data;
  }

  @Override
  public void checkProcessedBlockIds() {
    Set<Long> missingBlockIds = Sets.difference(expectedBlockIds, processedBlockIds);
    if (expectedBlockIds.size() != processedBlockIds.size() || !missingBlockIds.isEmpty()) {
      throw new RuntimeException("Blocks read inconsistent: expected " + expectedBlockIds.toString()
          + ", actual " + processedBlockIds.toString());
    }
  }

  @Override
  public void close() {
    if (shuffleReadHandler != null) {
      shuffleReadHandler.close();
    }
  }

  @Override
  public void logStatics() {
    LOG.info("Metrics for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]"
        + ", readIndexTime:" + readIndexTime + "ms, readDataTime:"
        + readDataTime + "ms for " + readSegments + " segments, checkBlockIdsTime:" + checkBlockIdsTime + "ms");
  }

  @VisibleForTesting
  protected Queue<Long> getBlockIdQueue() {
    return blockIdQueue;
  }
}
