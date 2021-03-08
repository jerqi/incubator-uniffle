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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleReadClientImpl implements ShuffleReadClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleReadClientImpl.class);

  private int shuffleId;
  private int partitionId;
  private Set<Long> expectedBlockIds;
  private Set<Long> processedBlockIds = Sets.newHashSet();
  private Set<Long> remainBlockIds = Sets.newHashSet();
  private Queue<BufferSegment> bufferSegmentQueue = Queues.newLinkedBlockingQueue();
  private AtomicLong readDataTime = new AtomicLong(0);
  private ClientReadHandler clientReadHandler;

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
      Set<Long> expectedBlockIds,
      List<ShuffleServerInfo> shuffleServerInfoList) {
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
    request.setPartitionsPerServer(partitionsPerServer);
    request.setPartitionNum(partitionNum);
    request.setReadBufferSize(readBufferSize);
    request.setStorageBasePath(storageBasePath);
    request.setExpectedBlockIds(expectedBlockIds);
    request.setShuffleServerInfoList(shuffleServerInfoList);
    clientReadHandler = ShuffleHandlerFactory.getInstance().createShuffleReadHandler(request);
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
        bufferSegmentQueue.addAll(sdr.getBufferSegments());
      }

      // get next buffer segment
      BufferSegment bs = null;
      do {
        // blocks in bufferSegmentQueue are from different partition, just read the necessary block
        bs = bufferSegmentQueue.poll();
      } while (bs != null && !remainBlockIds.contains(bs.getBlockId()));

      if (bs != null) {
        long expectedCrc = -1;
        long actualCrc = -1;

        if (bs.getByteBuffer() == null) {
          return null;
        }

        ByteBuffer byteBuffer = bs.getByteBuffer();
        try {
          expectedCrc = bs.getCrc();
          actualCrc = ChecksumUtils.getCrc32(byteBuffer);
          byteBuffer.clear();
        } catch (Exception e) {
          LOG.warn("Can't read data for blockId[" + bs.getBlockId() + "]", e);
        }
        if (expectedCrc != actualCrc) {
          throw new RuntimeException("Unexpected crc value for blockId[" + bs.getBlockId()
              + "], expected:" + expectedCrc + ", actual:" + actualCrc);
        }
        processedBlockIds.add(bs.getBlockId());
        remainBlockIds.remove(bs.getBlockId());
        return new CompressedShuffleBlock(bs.getByteBuffer(), bs.getUncompressLength());
      }
    }
    // all data are read
    return new CompressedShuffleBlock(null, 0);
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
    if (clientReadHandler != null) {
      clientReadHandler.close();
    }
  }

  @Override
  public void logStatics() {
    LOG.info("Metrics for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]"
        + ", readDataTime:" + readDataTime + " ms");
  }
}
