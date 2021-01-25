package com.tencent.rss.client.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.FileBasedShuffleReadHandler;
import com.tencent.rss.storage.FileBasedShuffleSegment;
import com.tencent.rss.storage.ShuffleStorageUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedShuffleReadClient implements ShuffleReadClient {

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedShuffleReadClient.class);

  private String basePath;
  private Configuration hadoopConf;
  private int indexReadLimit;
  private Set<Long> expectedBlockIds;
  private Map<String, FileBasedShuffleReadHandler> pathToHandler;
  private Map<Long, Queue<ShuffleSegmentWithPath>> blockIdToSegments = Maps.newHashMap();
  private Queue<Long> blockIdQueue = Queues.newLinkedBlockingQueue();
  private Set<Long> processedBlockIds = Sets.newHashSet();

  public FileBasedShuffleReadClient(
      String basePath, Configuration hadoopConf, int indexReadLimit, Set<Long> expectedBlockIds) {
    this.basePath = basePath;
    this.hadoopConf = hadoopConf;
    this.indexReadLimit = indexReadLimit;
    this.expectedBlockIds = expectedBlockIds;
  }

  @Override
  public void checkExpectedBlockIds() {
    FileSystem fs;
    Path baseFolder = new Path(basePath);
    try {
      fs = ShuffleStorageUtils.getFileSystemForPath(baseFolder, hadoopConf);
    } catch (IOException ioe) {
      throw new RuntimeException("Can't get FileSystem for " + basePath);
    }

    FileStatus[] indexFiles;
    String failedGetIndexFileMsg = "No index file found in  " + basePath;
    try {
      // get all index files
      indexFiles = fs.listStatus(baseFolder,
          file -> file.getName().endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX));
    } catch (Exception e) {
      throw new RuntimeException(failedGetIndexFileMsg);
    }

    if (indexFiles == null || indexFiles.length == 0) {
      throw new RuntimeException(failedGetIndexFileMsg);
    }

    pathToHandler = getPathToHandler(basePath, hadoopConf, indexFiles);
    Map<String, List<FileBasedShuffleSegment>> pathToSegments = readAllIndexSegments();
    updateBlockIdToSegments(pathToSegments, expectedBlockIds);
  }

  private Map<String, FileBasedShuffleReadHandler> getPathToHandler(
      String basePath, Configuration hadoopConf, FileStatus[] indexFiles) {
    Map<String, FileBasedShuffleReadHandler> pathToHandler = Maps.newHashMap();
    for (FileStatus status : indexFiles) {
      String fileNamePrefix = getFileNamePrefix(status.getPath().getName());
      try {
        pathToHandler.put(fileNamePrefix,
            new FileBasedShuffleReadHandler(basePath, fileNamePrefix, hadoopConf));
      } catch (Exception e) {
        LOG.warn("Can't create ShuffleReaderHandler for " + fileNamePrefix, e);
      }
    }
    return pathToHandler;
  }

  private String getFileNamePrefix(String fileName) {
    int point = fileName.lastIndexOf(".");
    return fileName.substring(0, point);
  }

  /**
   * Read all index files, and get all FileBasedShuffleSegment for every index file
   */
  private Map<String, List<FileBasedShuffleSegment>> readAllIndexSegments() {
    Map<String, List<FileBasedShuffleSegment>> pathToSegments = Maps.newHashMap();
    for (Entry<String, FileBasedShuffleReadHandler> entry : pathToHandler.entrySet()) {
      String path = entry.getKey();
      try {
        LOG.info("Read index file for: " + path);
        FileBasedShuffleReadHandler handler = entry.getValue();
        List<FileBasedShuffleSegment> segments = handler.readIndex(indexReadLimit);
        List<FileBasedShuffleSegment> allSegments = Lists.newArrayList();
        while (!segments.isEmpty()) {
          LOG.debug("Get segment : " + segments);
          allSegments.addAll(segments);
          segments = handler.readIndex(indexReadLimit);
        }
        pathToSegments.put(path, allSegments);
      } catch (Exception e) {
        LOG.warn("Can't read index segments for " + path, e);
      }
    }
    return pathToSegments;
  }

  /**
   * Parse every FileBasedShuffleSegment, and check if have all required blockIds
   */
  private void updateBlockIdToSegments(
      Map<String, List<FileBasedShuffleSegment>> pathToSegments,
      Set<Long> expectedBlockIds) {
    for (Entry<String, List<FileBasedShuffleSegment>> entry : pathToSegments.entrySet()) {
      String path = entry.getKey();
      LOG.info("Check file segment for " + path);
      for (FileBasedShuffleSegment segment : entry.getValue()) {
        long blockId = segment.getBlockId();
        LOG.debug("Find blockId " + blockId);
        if (expectedBlockIds.contains(blockId)) {
          if (blockIdToSegments.get(blockId) == null) {
            blockIdToSegments.put(blockId, Queues.newArrayDeque());
            blockIdQueue.add(blockId);
            LOG.info("Find needed blockId " + blockId);
          }
          blockIdToSegments.get(blockId).add(new ShuffleSegmentWithPath(path, segment));
        }
      }
    }
    Set<Long> copy = Sets.newHashSet(expectedBlockIds);
    copy.removeAll(blockIdToSegments.keySet());
    if (copy.size() > 0) {
      StringBuilder sb = new StringBuilder();
      for (Long blockId : expectedBlockIds) {
        sb.append(blockId).append(",");
      }
      throw new RuntimeException("Can't find blockIds " + copy.toString() + ", expected[" + sb.toString() + "]");
    }
  }

  @Override
  public byte[] readShuffleData() {
    // get next blockId
    Long blockId = blockIdQueue.poll();
    if (blockId == null) {
      return null;
    }
    // get segment queue
    Queue<ShuffleSegmentWithPath> segmentQueue = blockIdToSegments.get(blockId);
    ShuffleSegmentWithPath segment = segmentQueue.poll();
    if (segment == null) {
      throw new RuntimeException("Can't read data with blockId:" + blockId);
    }
    byte[] data = null;
    long expectedLength = -1;
    long expectedCrc = -1;
    long actualCrc = -1;
    boolean readSuccess = false;
    while (!readSuccess) {
      try {
        FileBasedShuffleSegment fss = segment.segment;
        data = pathToHandler.get(segment.path).readData(fss);
        expectedLength = fss.getLength();
        expectedCrc = fss.getCrc();
        readSuccess = true;
        actualCrc = ChecksumUtils.getCrc32(data);
      } catch (Exception e) {
        // read failed for current segment, try next one if possible
        LOG.warn("Can't read data from " + segment.path + "["
            + segment.segment + "] for blockId[" + blockId + "]", e);
        segment = segmentQueue.poll();
        if (segment == null) {
          throw new RuntimeException("Can't read data with blockId:" + blockId);
        }
      }
    }
    if (data == null) {
      throw new RuntimeException("Can't read data for blockId[" + blockId + "]");
    } else if (data.length != expectedLength) {
      throw new RuntimeException("Unexpected data length for blockId[" + blockId
          + "], expected:" + expectedLength + ", actual:" + data.length);
    }
    if (expectedCrc != actualCrc) {
      throw new RuntimeException("Unexpected crc value for blockId[" + blockId
          + "], expected:" + expectedCrc + ", actual:" + actualCrc);
    }
    processedBlockIds.add(blockId);
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
    if (pathToHandler != null) {
      for (Entry<String, FileBasedShuffleReadHandler> entry : pathToHandler.entrySet()) {
        try {
          entry.getValue().close();
        } catch (Exception e) {
          LOG.warn("Can't close reader successfully for " + entry.getKey(), e);
        }
      }
    }
  }

  @VisibleForTesting
  protected Queue<Long> getBlockIdQueue() {
    return blockIdQueue;
  }

  class ShuffleSegmentWithPath {

    String path;
    FileBasedShuffleSegment segment;

    ShuffleSegmentWithPath(String path, FileBasedShuffleSegment segment) {
      this.path = path;
      this.segment = segment;
    }
  }
}
