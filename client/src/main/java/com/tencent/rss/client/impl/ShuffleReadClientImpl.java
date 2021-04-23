package com.tencent.rss.client.impl;

import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ClientReadHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleReadClientImpl implements ShuffleReadClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleReadClientImpl.class);

  private int shuffleId;
  private int partitionId;
  private byte[] readBuffer;
  private Set<Long> expectedBlockIds;
  private Set<Long> processedBlockIds = Sets.newHashSet();
  private Set<Long> remainBlockIds = Sets.newHashSet();
  private Queue<BufferSegment> bufferSegmentQueue = Queues.newLinkedBlockingQueue();
  private AtomicLong readDataTime = new AtomicLong(0);
  private AtomicLong copyTime = new AtomicLong(0);
  private AtomicLong crcCheckTime = new AtomicLong(0);
  private ClientReadHandler clientReadHandler;

  public ShuffleReadClientImpl(
      String storageType,
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      String storageBasePath,
      Set<Long> expectedBlockIds,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf) {
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.expectedBlockIds = expectedBlockIds;
    this.remainBlockIds = Sets.newHashSet(expectedBlockIds);
    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setStorageType(storageType);
    request.setAppId(appId);
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setIndexReadLimit(indexReadLimit);
    request.setPartitionNumPerRange(partitionNumPerRange);
    request.setPartitionNum(partitionNum);
    request.setReadBufferSize(readBufferSize);
    request.setStorageBasePath(storageBasePath);
    request.setExpectedBlockIds(expectedBlockIds);
    request.setShuffleServerInfoList(shuffleServerInfoList);
    request.setHadoopConf(hadoopConf);
    clientReadHandler = ShuffleHandlerFactory.getInstance().createShuffleReadHandler(request);
  }

  public ShuffleReadClientImpl(
      String storageType,
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      String storageBasePath,
      Set<Long> expectedBlockIds,
      List<ShuffleServerInfo> shuffleServerInfoList) {
    this(storageType,
        appId,
        shuffleId,
        partitionId,
        indexReadLimit,
        partitionNumPerRange,
        partitionNum,
        readBufferSize,
        storageBasePath,
        expectedBlockIds,
        shuffleServerInfoList,
        new Configuration());
  }

  @Override
  public CompressedShuffleBlock readShuffleBlockData() {
    // try read data
    while (!remainBlockIds.isEmpty()) {
      // if need request new buff
      if (bufferSegmentQueue.isEmpty()) {
        long start = System.currentTimeMillis();
        ShuffleDataResult sdr = clientReadHandler.readShuffleData(remainBlockIds);
        readDataTime.addAndGet(System.currentTimeMillis() - start);
        readBuffer = sdr.getData();
        if (readBuffer == null) {
          // there is no data, return
          return null;
        }
        bufferSegmentQueue.addAll(sdr.getBufferSegments());
      }
      // get next buffer segment
      BufferSegment bs = null;
      do {
        // blocks in bufferSegmentQueue are from different partition, just read the necessary block
        bs = bufferSegmentQueue.poll();
      } while (bs != null && !remainBlockIds.contains(bs.getBlockId()));
      byte[] data = null;
      if (bs != null) {
        data = new byte[bs.getLength()];
        long expectedCrc = -1;
        long actualCrc = -1;
        try {
          long start = System.currentTimeMillis();
          System.arraycopy(readBuffer, bs.getOffset(), data, 0, bs.getLength());
          copyTime.addAndGet(System.currentTimeMillis() - start);
          start = System.currentTimeMillis();
          expectedCrc = bs.getCrc();
          actualCrc = ChecksumUtils.getCrc32(data);
          crcCheckTime.addAndGet(System.currentTimeMillis() - start);
        } catch (Exception e) {
          LOG.warn("Can't read data for blockId[" + bs.getBlockId() + "]", e);
        }
        if (expectedCrc != actualCrc) {
          throw new RuntimeException("Unexpected crc value for blockId[" + bs.getBlockId()
              + "], expected:" + expectedCrc + ", actual:" + actualCrc);
        }
        processedBlockIds.add(bs.getBlockId());
        remainBlockIds.remove(bs.getBlockId());
        return new CompressedShuffleBlock(ByteBuffer.wrap(data), bs.getUncompressLength());
      }
    }
    // all data are read
    return new CompressedShuffleBlock(null, 0);
  }

  @Override
  public void checkProcessedBlockIds() {
    Set<Long> missingBlockIds = Sets.difference(expectedBlockIds, processedBlockIds);
    if (expectedBlockIds.size() != processedBlockIds.size() || !missingBlockIds.isEmpty()) {
      throw new RuntimeException("Blocks read inconsistent: expected " + expectedBlockIds.size()
          + " blocks, actual " + processedBlockIds.size()
          + " blocks, missing " + missingBlockIds.size() + " blocks");
    }
  }

  @Override
  public void close() {
    if (clientReadHandler != null) {
      clientReadHandler.close();
    }
  }

  @Override
  public void logStatics() {
    LOG.info("Metrics for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]"
        + ", read data cost " + readDataTime + " ms, copy data cost " + copyTime
        + " ms, crc check cost " + crcCheckTime + " ms");
  }
}
