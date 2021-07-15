package com.tencent.rss.client.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.handler.api.ClientReadHandler;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleReadClientImpl implements ShuffleReadClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleReadClientImpl.class);

  private int shuffleId;
  private int partitionId;
  private int indexReadLimit;
  private byte[] readBuffer;
  private Roaring64NavigableMap blockIdBitmap;
  private Roaring64NavigableMap taskIdBitmap;
  private Roaring64NavigableMap processedBlockIds = Roaring64NavigableMap.bitmapOf();
  private Queue<BufferSegment> bufferSegmentQueue = Queues.newLinkedBlockingQueue();
  private Set<Long> remainBlockIds = Sets.newHashSet();
  private AtomicLong readDataTime = new AtomicLong(0);
  private AtomicLong copyTime = new AtomicLong(0);
  private AtomicLong crcCheckTime = new AtomicLong(0);
  private LongIterator blockIdIter;
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
      Roaring64NavigableMap blockIdBitmap,
      Roaring64NavigableMap taskIdBitmap,
      List<ShuffleServerInfo> shuffleServerInfoList,
      Configuration hadoopConf) {
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.blockIdBitmap = blockIdBitmap;
    this.taskIdBitmap = taskIdBitmap;
    this.blockIdIter = blockIdBitmap.getLongIterator();
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
    request.setBlockIdBitmap(blockIdBitmap);
    request.setShuffleServerInfoList(shuffleServerInfoList);
    request.setHadoopConf(hadoopConf);
    clientReadHandler = ShuffleHandlerFactory.getInstance().createShuffleReadHandler(request);
  }

  @Override
  public CompressedShuffleBlock readShuffleBlockData() {
    if (remainBlockIds.isEmpty()) {
      remainBlockIds = getNextBatchBlockIds();
      LOG.info("Got next batch with " + remainBlockIds.size() + " blockIds for shuffleId["
          + shuffleId + "], partitionId[" + partitionId + "]");
    }
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
        // blocks in bufferSegmentQueue may be from different partition in range partition mode,
        // or may be from speculation task, filter them and just read the necessary block
        bs = bufferSegmentQueue.poll();
        // the segment is generated from unexpected task, filter it and mark it as processed
        if (bs != null && !taskIdBitmap.contains(bs.getTaskAttemptId())) {
          processedBlockIds.addLong(bs.getBlockId());
          remainBlockIds.remove(bs.getBlockId());
        }
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
        processedBlockIds.addLong(bs.getBlockId());
        remainBlockIds.remove(bs.getBlockId());
        return new CompressedShuffleBlock(ByteBuffer.wrap(data), bs.getUncompressLength());
      }
    }
    // all data are read
    return new CompressedShuffleBlock(null, 0);
  }

  private Set<Long> getNextBatchBlockIds() {
    int i = 1;
    Set<Long> blockIds = Sets.newHashSet();
    while (blockIdIter.hasNext()) {
      blockIds.add(blockIdIter.next());
      i++;
      if (i > indexReadLimit) {
        break;
      }
    }
    return blockIds;
  }

  @VisibleForTesting
  protected Roaring64NavigableMap getProcessedBlockIds() {
    return processedBlockIds;
  }

  @Override
  public void checkProcessedBlockIds() {
    Roaring64NavigableMap cloneBitmap;
    try {
      cloneBitmap = RssUtils.deserializeBitMap(RssUtils.serializeBitMap(blockIdBitmap));
    } catch (IOException ioe) {
      throw new RuntimeException("Can't validate processed blockIds.", ioe);
    }
    cloneBitmap.and(processedBlockIds);
    if (!blockIdBitmap.equals(cloneBitmap)) {
      throw new RuntimeException("Blocks read inconsistent: expected " + blockIdBitmap.getLongCardinality()
          + " blocks, actual " + cloneBitmap.getLongCardinality() + " blocks");
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
