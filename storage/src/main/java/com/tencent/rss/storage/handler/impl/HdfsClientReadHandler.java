package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.api.ShuffleReader;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.IOException;
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

public class HdfsClientReadHandler extends AbstractFileClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsClientReadHandler.class);
  private final int indexReadLimit;
  private Map<String, HdfsFileReader> dataReaderMap = Maps.newHashMap();
  private Map<String, HdfsFileReader> indexReaderMap = Maps.newHashMap();
  private Set<Long> expectedBlockIds = Sets.newHashSet();
  private List<FileReadSegment> fileReadSegments = Lists.newArrayList();
  private int partitionsPerServer;
  private int partitionNum;
  private int readBufferSize;
  private String storageBasePath;
  private AtomicLong readIndexTime = new AtomicLong(0);
  private AtomicLong readDataTime = new AtomicLong(0);

  public HdfsClientReadHandler(
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
    this.expectedBlockIds = expectedBlockIds;
    if (expectedBlockIds != null && !expectedBlockIds.isEmpty()) {
      init();
    }
  }

  private void init() {
    String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPathWithRange(appId,
            shuffleId, partitionId, partitionsPerServer, partitionNum));

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
    Set<Long> blockIds = Sets.newHashSet();
    for (Entry<String, HdfsFileReader> entry : indexReaderMap.entrySet()) {
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
        fileReadSegments.addAll(ShuffleStorageUtils.mergeSegments(path, allSegments, readBufferSize));
      } catch (Exception e) {
        LOG.warn("Can't read index segments for " + path, e);
      }
    }

    if (!blockIds.containsAll(expectedBlockIds)) {
      Set<Long> copy = Sets.newHashSet(expectedBlockIds);
      copy.removeAll(blockIds);
      throw new RuntimeException("Can't find blockIds " + copy + ", expected[" + expectedBlockIds + "]");
    }
  }

  @Override
  public ShuffleDataResult readShuffleData(Set<Long> blockIds) {
    byte[] readBuffer = null;
    List<BufferSegment> bufferSegments = Lists.newArrayList();
    if (fileReadSegments != null) {
      // missing some blocks, keep reading
      if (blockIds.size() > 0) {
        for (FileReadSegment fileSegment : fileReadSegments) {
          Set<Long> leftIds = Sets.newHashSet(blockIds);
          leftIds.retainAll(fileSegment.getBlockIds());
          // if the fileSegment has missing blocks
          if (leftIds.size() > 0) {
            try {
              long start = System.currentTimeMillis();
              readBuffer = dataReaderMap.get(fileSegment.getPath()).readData(
                  new FileBasedShuffleSegment(0, fileSegment.getOffset(), fileSegment.getLength(), 0));
              LOG.info("Read File segment: " + fileSegment.getPath() + ", offset["
                  + fileSegment.getOffset() + "], length[" + fileSegment.getLength()
                  + "], cost:" + (System.currentTimeMillis() - start) + " ms, for " + leftIds);
              readDataTime.addAndGet(System.currentTimeMillis() - start);
              bufferSegments.addAll(fileSegment.getBufferSegments());
              break;
            } catch (Exception e) {
              LOG.warn("Can't read data for " + fileSegment.getPath() + ", offset["
                  + fileSegment.getOffset() + "], length[" + fileSegment.getLength() + "]");
            }
          }
        }
      }
    }
    return new ShuffleDataResult(readBuffer, bufferSegments);
  }

  @Override
  public synchronized void close() {
    for (Map.Entry<String, HdfsFileReader> entry : dataReaderMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException ioe) {
        String message = "Error happened when close FileBasedShuffleReader for " + entry.getKey() + ".data";
        LOG.warn(message, ioe);
      }
    }

    for (Map.Entry<String, HdfsFileReader> entry : indexReaderMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException ioe) {
        String message = "Error happened when close FileBasedShuffleReader for " + entry.getKey() + ".index";
        LOG.warn(message, ioe);
      }
    }
  }

  private HdfsFileReader createHdfsReader(
      String folder, String fileName, Configuration hadoopConf) throws IOException, IllegalStateException {
    Path path = new Path(folder, fileName);
    HdfsFileReader reader = new HdfsFileReader(path, hadoopConf);
    reader.createStream();
    return reader;
  }
}
