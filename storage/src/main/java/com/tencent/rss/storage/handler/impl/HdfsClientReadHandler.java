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
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
  private Map<Long, FileReadSegment> allSegments = Maps.newHashMap();
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
        while (!segments.isEmpty()) {
          LOG.debug("Get segment : " + segments);
          for (FileBasedShuffleSegment segment : segments) {
            blockIds.add(segment.getBlockId());
            allSegments.put(segment.getBlockId(), new FileReadSegment(segment, path));
          }
          segments = reader.readIndex(indexReadLimit);
        }
        readIndexTime.addAndGet((System.currentTimeMillis() - start));
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
    ShuffleDataResult shuffleDataResult = new ShuffleDataResult();
    List<BufferSegment> bufferSegments = Lists.newArrayList();

    List<Long> sortedBlockId = new ArrayList<>(blockIds);
    sortedBlockId.sort((b1, b2) -> Long.compare(allSegments.get(b1).getOffset(), allSegments.get(b2).getOffset()));

    if (!allSegments.isEmpty()) {
      long curSize = 0L;
      for (Long blockId : sortedBlockId) {
        if (allSegments.containsKey(blockId)) {
          FileReadSegment fileSegment = allSegments.get(blockId);
          try {
            long start = System.currentTimeMillis();
            ByteBuffer data = dataReaderMap.get(fileSegment.getPath()).readData(
                fileSegment.getOffset(), fileSegment.getLength());
            LOG.info("Read File segment: " + fileSegment.getPath() + ", offset["
                + fileSegment.getOffset() + "], length[" + fileSegment.getLength()
                + "], cost:" + (System.currentTimeMillis() - start) + " ms, for " + blockId);
            readDataTime.addAndGet(System.currentTimeMillis() - start);
            bufferSegments.add(new BufferSegment(
                blockId,
                fileSegment.getOffset(),
                fileSegment.getLength(),
                fileSegment.getUncompressLength(),
                fileSegment.getCrc(),
                data));
            curSize += fileSegment.getLength();
            if (curSize >= readBufferSize) {
              break;
            }
          } catch (Exception e) {
            LOG.warn("Can't read data for " + fileSegment.getPath() + ", offset["
                + fileSegment.getOffset() + "], length[" + fileSegment.getLength() + "]");
          }
        }
      }
      shuffleDataResult = new ShuffleDataResult(bufferSegments);
    }
    return shuffleDataResult;
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
    return reader;
  }
}
