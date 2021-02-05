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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
  private long readBufferSize;
  private byte[] readBuffer;
  private Set<Long> expectedBlockIds;
  private Map<String, FileBasedShuffleReadHandler> pathToHandler;
  private Map<String, List<FileBasedShuffleSegment>> pathToSegments = Maps.newHashMap();
  private List<FileReadSegment> fileReadSegments = Lists.newArrayList();
  private Queue<Long> blockIdQueue = Queues.newLinkedBlockingQueue();
  private Set<Long> processedBlockIds = Sets.newHashSet();
  private Map<Long, BufferSegment> processingBlockIds = Maps.newHashMap();
  private AtomicInteger readSegments = new AtomicInteger(0);
  private AtomicLong readDataTime = new AtomicLong(0);
  private AtomicLong readIndexTime = new AtomicLong(0);
  private AtomicLong checkBlockIdsTime = new AtomicLong(0);

  public FileBasedShuffleReadClient(
      String basePath, Configuration hadoopConf, int indexReadLimit,
      int readBufferSize, Set<Long> expectedBlockIds) {
    this.basePath = basePath;
    this.hadoopConf = hadoopConf;
    this.indexReadLimit = indexReadLimit;
    this.readBufferSize = readBufferSize;
    this.expectedBlockIds = expectedBlockIds;
  }

  @Override
  public void checkExpectedBlockIds() {
    // there is no data
    if (expectedBlockIds == null || expectedBlockIds.isEmpty()) {
      LOG.info("There is no shuffle data for " + basePath);
      return;
    }
    final long start = System.currentTimeMillis();
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
    readAllIndexSegments();
    verifyBlockIds(pathToSegments, expectedBlockIds);
    checkBlockIdsTime.addAndGet(System.currentTimeMillis() - start);
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
  private void readAllIndexSegments() {
    for (Entry<String, FileBasedShuffleReadHandler> entry : pathToHandler.entrySet()) {
      String path = entry.getKey();
      try {
        LOG.info("Read index file for: " + path);
        FileBasedShuffleReadHandler handler = entry.getValue();
        long start = System.currentTimeMillis();
        List<FileBasedShuffleSegment> segments = handler.readIndex(indexReadLimit);
        List<FileBasedShuffleSegment> allSegments = Lists.newArrayList();
        Set<Long> blockIds = Sets.newHashSet();
        while (!segments.isEmpty()) {
          LOG.debug("Get segment : " + segments);
          allSegments.addAll(segments);
          for (FileBasedShuffleSegment segment : segments) {
            long blockId = segment.getBlockId();
            blockIds.add(blockId);
          }
          segments = handler.readIndex(indexReadLimit);
        }
        readIndexTime.addAndGet((System.currentTimeMillis() - start));
        pathToSegments.put(path, allSegments);
        fileReadSegments.addAll(mergeSegments(path, allSegments));
      } catch (Exception e) {
        LOG.warn("Can't read index segments for " + path, e);
      }
    }
  }

  /**
   * Parse every FileBasedShuffleSegment, and check if have all required blockIds
   */
  private void verifyBlockIds(
      Map<String, List<FileBasedShuffleSegment>> pathToSegments,
      Set<Long> expectedBlockIds) {
    Set<Long> actualBlockIds = Sets.newHashSet();
    for (Entry<String, List<FileBasedShuffleSegment>> entry : pathToSegments.entrySet()) {
      String path = entry.getKey();
      LOG.info("Check file segment for " + path);
      for (FileBasedShuffleSegment segment : entry.getValue()) {
        long blockId = segment.getBlockId();
        LOG.debug("Find blockId " + blockId);
        if (expectedBlockIds.contains(blockId)) {
          actualBlockIds.add(blockId);
        }
      }
    }
    Set<Long> copy = Sets.newHashSet(expectedBlockIds);
    if (actualBlockIds.size() < expectedBlockIds.size()) {
      copy.removeAll(expectedBlockIds);
      throw new RuntimeException("Can't find blockIds " + copy + ", expected[" + expectedBlockIds + "]");
    }
  }

  private void readFileSegment() {
    processingBlockIds = Maps.newHashMap();
    if (fileReadSegments != null) {
      Set<Long> expectedBlockIdsCp = Sets.newHashSet(expectedBlockIds);
      expectedBlockIdsCp.removeAll(processedBlockIds);
      // missing some blocks, keep reading
      if (expectedBlockIdsCp.size() > 0) {
        for (FileReadSegment fileSegment : fileReadSegments) {
          Set<Long> leftIds = Sets.newHashSet(expectedBlockIdsCp);
          leftIds.retainAll(fileSegment.blockIdToBufferSegment.keySet());
          // if the fileSegment has missing blocks
          if (leftIds.size() > 0) {
            try {
              long start = System.currentTimeMillis();
              readBuffer = pathToHandler.get(fileSegment.path).readData(
                  new FileBasedShuffleSegment(fileSegment.offset, fileSegment.length, 0, 0));
              LOG.info("Read File segment: " + fileSegment.path + ", offset["
                  + fileSegment.offset + "], length[" + fileSegment.length
                  + "], cost:" + (System.currentTimeMillis() - start) + " ms, for " + leftIds);
              readDataTime.addAndGet(System.currentTimeMillis() - start);
              for (Long blockId : leftIds) {
                processingBlockIds.put(blockId, fileSegment.blockIdToBufferSegment.get(blockId));
                blockIdQueue.add(blockId);
              }
              break;
            } catch (IOException e) {
              LOG.warn("Can't read data for " + fileSegment.path + ", offset["
                  + fileSegment.offset + "], length[" + fileSegment.length + "]");
            }
          }
        }
      }
    }
  }

  @Override
  public byte[] readShuffleData() {
    // all blocks are read, return
    if (expectedBlockIds.size() == processedBlockIds.size()) {
      return null;
    }
    if (processingBlockIds.isEmpty()) {
      readFileSegment();
    }
    // get next blockId
    Long blockId = blockIdQueue.poll();
    if (blockId == null) {
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

  @Override
  public void logStatics() {
    LOG.info("Metrics for path " + basePath + ", readIndexTime:" + readIndexTime + "ms, readDataTime:"
        + readDataTime + "ms for " + readSegments + " segments, checkBlockIdsTime:" + checkBlockIdsTime + "ms");
  }

  @VisibleForTesting
  protected Queue<Long> getBlockIdQueue() {
    return blockIdQueue;
  }

  @VisibleForTesting
  protected List<FileReadSegment> mergeSegments(String path, List<FileBasedShuffleSegment> segments) {
    List<FileReadSegment> fileReadSegments = Lists.newArrayList();
    if (segments != null && !segments.isEmpty()) {
      if (segments.size() == 1) {
        Map<Long, BufferSegment> btb = Maps.newHashMap();
        btb.put(segments.get(0).getBlockId(), new BufferSegment(0,
            segments.get(0).getLength(), segments.get(0).getCrc()));
        fileReadSegments.add(new FileReadSegment(
            path, segments.get(0).getOffset(), segments.get(0).getLength(), btb));
      } else {
        Collections.sort(segments);
        long start = -1;
        long lastestPosition = -1;
        long skipThreshold = readBufferSize / 2;
        long lastPosition = Long.MAX_VALUE;
        Map<Long, BufferSegment> btb = Maps.newHashMap();
        for (FileBasedShuffleSegment segment : segments) {
          // check if there has expected skip range, eg, [20, 100], [1000, 1001] and the skip range is [101, 999]
          if (start > -1 && segment.getOffset() - lastPosition > skipThreshold) {
            fileReadSegments.add(new FileReadSegment(
                path, start, lastPosition - start, btb));
            start = -1;
          }
          // previous FileBasedShuffleSegment are merged, start new merge process
          if (start == -1) {
            btb = Maps.newHashMap();
            start = segment.getOffset();
          }
          lastestPosition = segment.getOffset() + segment.getLength();
          btb.put(segment.getBlockId(), new BufferSegment(segment.getOffset() - start,
              segment.getLength(), segment.getCrc()));
          if (lastestPosition - start >= readBufferSize) {
            fileReadSegments.add(new FileReadSegment(
                path, start, lastestPosition - start, btb));
            start = -1;
          }
          lastPosition = lastestPosition;
        }
        if (start > -1) {
          fileReadSegments.add(new FileReadSegment(path, start, lastestPosition - start, btb));
        }
      }
    }
    return fileReadSegments;
  }

  class FileReadSegment {

    String path;
    long offset;
    long length;
    Map<Long, BufferSegment> blockIdToBufferSegment;

    FileReadSegment(String path, long offset, long length, Map<Long, BufferSegment> blockIdToBufferSegment) {
      this.path = path;
      this.offset = offset;
      this.length = length;
      this.blockIdToBufferSegment = blockIdToBufferSegment;
    }
  }
}
