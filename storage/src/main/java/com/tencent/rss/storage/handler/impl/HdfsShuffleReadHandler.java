package com.tencent.rss.storage.handler.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.api.ShuffleReader;
import com.tencent.rss.storage.common.BufferSegment;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.utils.ShuffleStorageUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsShuffleReadHandler extends AbstractFileShuffleReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsShuffleReadHandler.class);
  private final int indexReadLimit;
  private Map<String, FileBasedShuffleReader> dataReaderMap = Maps.newHashMap();
  private Map<String, FileBasedShuffleReader> indexReaderMap = Maps.newHashMap();
  private Set<Long> blockIds = Sets.newHashSet();
  private List<FileReadSegment> fileReadSegments = Lists.newArrayList();
  private int partitionsPerServer;
  private int partitionNum;
  private int readBufferSize;
  private String storageBasePath;
  private Map<Long, BufferSegment> blockIdToBufferSegment = Maps.newHashMap();
  //  private Map<String, List<FileBasedShuffleSegment>> pathToSegments = Maps.newHashMap();
  private AtomicLong readIndexTime = new AtomicLong(0);
  private AtomicLong readDataTime = new AtomicLong(0);

  public HdfsShuffleReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionsPerServer,
      int partitionNum,
      int readBufferSize,
      String storageBasePath,
      Set<Long> expectedBlockIds) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionsPerServer = partitionsPerServer;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.storageBasePath = storageBasePath;
    if (expectedBlockIds != null && !expectedBlockIds.isEmpty()) {
      init();
    }
  }

  private void init() {
    String fullShufflePath = RssUtils.getFullShuffleDataFolder(storageBasePath,
        RssUtils.getShuffleDataPathWithRange(appId, shuffleId, partitionId, partitionsPerServer, partitionNum));

    FileSystem fs;
    Path baseFolder = new Path(fullShufflePath);
    Configuration hadoopConf = new Configuration();
    try {
      fs = ShuffleStorageUtils.getFileSystemForPath(baseFolder, hadoopConf);
    } catch (IOException ioe) {
      throw new RuntimeException("Can't get FileSystem for " + baseFolder);
    }

    FileStatus[] indexFiles;
    String failedGetIndexFileMsg = "No index file found in  " + baseFolder;
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

    for (FileStatus status : indexFiles) {
      String fileNamePrefix = getFileNamePrefix(status.getPath().getName());
      try {
        dataReaderMap.put(fileNamePrefix,
            createHdfsReader(fullShufflePath, ShuffleStorageUtils.generateDataFileName(fileNamePrefix), hadoopConf));
        indexReaderMap.put(fileNamePrefix,
            createHdfsReader(fullShufflePath, ShuffleStorageUtils.generateIndexFileName(fileNamePrefix), hadoopConf));
      } catch (Exception e) {
        LOG.warn("Can't create ShuffleReaderHandler for " + fileNamePrefix, e);
      }
    }

    readAllIndexSegments();
  }

  private String getFileNamePrefix(String fileName) {
    int point = fileName.lastIndexOf(".");
    return fileName.substring(0, point);
  }

  /**
   * Read all index files, and get all FileBasedShuffleSegment for every index file
   */
  private void readAllIndexSegments() {
    for (Entry<String, FileBasedShuffleReader> entry : indexReaderMap.entrySet()) {
      String path = entry.getKey();
      try {
        LOG.info("Read index file for: " + path);
        ShuffleReader reader = entry.getValue();
        long start = System.currentTimeMillis();
        List<FileBasedShuffleSegment> segments = reader.readIndex(indexReadLimit);
        List<FileBasedShuffleSegment> allSegments = Lists.newArrayList();
        while (!segments.isEmpty()) {
          LOG.debug("Get segment : " + segments);
          allSegments.addAll(segments);
          for (FileBasedShuffleSegment segment : segments) {
            blockIds.add(segment.getBlockId());
          }
          segments = reader.readIndex(indexReadLimit);
        }
        readIndexTime.addAndGet((System.currentTimeMillis() - start));
//        pathToSegments.put(path, allSegments);
        fileReadSegments.addAll(mergeSegments(path, allSegments));
      } catch (Exception e) {
        LOG.warn("Can't read index segments for " + path, e);
      }
    }
  }

  @Override
  public byte[] readShuffleData(Set<Long> blockIds) {
    byte[] readBuffer = null;
    blockIdToBufferSegment.clear();
    if (fileReadSegments != null) {
      // missing some blocks, keep reading
      if (blockIds.size() > 0) {
        for (FileReadSegment fileSegment : fileReadSegments) {
          Set<Long> leftIds = Sets.newHashSet(blockIds);
          leftIds.retainAll(fileSegment.getBlockIdToBufferSegment().keySet());
          // if the fileSegment has missing blocks
          if (leftIds.size() > 0) {
            try {
              long start = System.currentTimeMillis();
              readBuffer = dataReaderMap.get(fileSegment.getPath()).readData(
                  new FileBasedShuffleSegment(fileSegment.getOffset(), fileSegment.getLength(), 0, 0));
              LOG.info("Read File segment: " + fileSegment.getPath() + ", offset["
                  + fileSegment.getOffset() + "], length[" + fileSegment.getLength()
                  + "], cost:" + (System.currentTimeMillis() - start) + " ms, for " + leftIds);
              readDataTime.addAndGet(System.currentTimeMillis() - start);
              for (Long blockId : leftIds) {
                blockIdToBufferSegment.put(blockId, fileSegment.getBlockIdToBufferSegment().get(blockId));
              }
              break;
            } catch (Exception e) {
              LOG.warn("Can't read data for " + fileSegment.getPath() + ", offset["
                  + fileSegment.getOffset() + "], length[" + fileSegment.getLength() + "]");
            }
          }
        }
      }
    }
    return readBuffer;
  }

  @Override
  public Map<Long, BufferSegment> getBlockIdToBufferSegment() {
    return blockIdToBufferSegment;
  }


  @Override
  public Set<Long> getAllBlockIds() {
    return blockIds;
  }

  @Override
  public synchronized void close() {
    for (Map.Entry<String, FileBasedShuffleReader> entry : dataReaderMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException ioe) {
        String message = "Error happened when close FileBasedShuffleReader for " + entry.getKey() + ".data";
        LOG.warn(message, ioe);
      }
    }

    for (Map.Entry<String, FileBasedShuffleReader> entry : indexReaderMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException ioe) {
        String message = "Error happened when close FileBasedShuffleReader for " + entry.getKey() + ".index";
        LOG.warn(message, ioe);
      }
    }
  }

  private FileBasedShuffleReader createHdfsReader(
      String folder, String fileName, Configuration hadoopConf) throws IOException, IllegalStateException {
    Path path = new Path(folder, fileName);
    FileBasedShuffleReader reader = new FileBasedShuffleReader(path, hadoopConf);
    reader.createStream();
    return reader;
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
}
