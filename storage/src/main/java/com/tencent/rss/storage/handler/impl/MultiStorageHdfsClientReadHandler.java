package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class MultiStorageHdfsClientReadHandler extends AbstractHdfsClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStorageHdfsClientReadHandler.class);

  public MultiStorageHdfsClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      String storageBasePath,
      Set<Long> expectedBlockIds,
      Configuration hadoopConf) {
      this.appId = appId;
      this.shuffleId = shuffleId;
      this.partitionId = partitionId;
      this.indexReadLimit = indexReadLimit;
      this.partitionNumPerRange = partitionNumPerRange;
      this.partitionNum = partitionNum;
      this.readBufferSize = readBufferSize;
      this.storageBasePath = storageBasePath;
      this.expectedBlockIds = expectedBlockIds;
      this.hadoopConf = hadoopConf;
    if (expectedBlockIds != null && !expectedBlockIds.isEmpty()) {
      try {
        String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
            ShuffleStorageUtils.getUploadShuffleDataPath(appId, shuffleId, partitionId));
        init(fullShufflePath);
      } catch (RuntimeException e) {
        String combinePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
            ShuffleStorageUtils.getCombineDataPath(appId, shuffleId));
        init(combinePath);
      }
      readAllIndexSegments();
    }
  }

  @Override
  protected void readAllIndexSegments() {
    Set<Long> blockIds = Sets.newHashSet();
    for (Map.Entry<String, HdfsFileReader> entry : indexReaderMap.entrySet()) {
      String path = entry.getKey();
      try {
        int limit = indexReadLimit;
        LOG.info("Read index file for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "] with " + path);
        HdfsFileReader reader = entry.getValue();
        ShuffleIndexHeader header = reader.readHeader();
        List<FileBasedShuffleSegment> allSegments = Lists.newArrayList();
        long start = System.currentTimeMillis();
        long lastPos = 0;
        Queue<ShuffleIndexHeader.Entry> indexes = header.getIndexes();
        while (!indexes.isEmpty()) {
          ShuffleIndexHeader.Entry indexEntry = indexes.poll();
          long length = indexEntry.getValue() / FileBasedShuffleSegment.BYTES;
          if (indexReadLimit > length) {
            limit = (int)length;
          }
          long segmentLength = 0;
          for (int i = 0; i < length; i = i + limit) {
            List<FileBasedShuffleSegment> segments = reader.readIndex(limit);
            if (indexEntry.getKey() == partitionId) {
              allSegments.addAll(segments);
            }
            for (FileBasedShuffleSegment segment : segments) {
              segmentLength = segmentLength + segment.getLength();
              if (indexEntry.getKey() == partitionId) {
                segment.setOffset(segment.getOffset() + lastPos);
                blockIds.add(segment.getBlockId());
              }
            }
          }
          lastPos = lastPos + segmentLength;
        }
        readIndexTime.addAndGet((System.currentTimeMillis() - start));
        fileReadSegments.addAll(ShuffleStorageUtils.mergeSegments(path, allSegments, readBufferSize));
      } catch (Exception e) {
        LOG.warn("Can't read index segments for " + path, e);
      }
    }

    if (!blockIds.containsAll(expectedBlockIds)) {
      Set<Long> copy = Sets.newHashSet(expectedBlockIds);
      copy.removeAll(blockIds);
      throw new RuntimeException("Missing " + copy.size() + " blockIds, expected "
          + expectedBlockIds.size() + " blockIds");
    }
  }
}
