package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.api.ServerReadHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileServerReadHandler implements ServerReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileServerReadHandler.class);
  private Map<String, String> indexPathMap = Maps.newHashMap();
  private Map<String, String> dataPathMap = Maps.newHashMap();
  private Map<Long, FileReadSegment> allSegments = Maps.newHashMap();
  private int readBufferSize;
  private AtomicLong readDataTime = new AtomicLong(0);
  private String appId;
  private int shuffleId;
  private int partitionId;

  public LocalFileServerReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int partitionsPerServer,
      int partitionNum,
      int readBufferSize,
      Set<Long> expectedBlockIds,
      RssBaseConf rssBaseConf) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    if (expectedBlockIds != null && !expectedBlockIds.isEmpty()) {
      init(appId, shuffleId, partitionId, partitionsPerServer, partitionNum,
          rssBaseConf, expectedBlockIds);
    }
  }

  private void init(
      String appId,
      int shuffleId,
      int partitionId,
      int partitionsPerServer,
      int partitionNum,
      RssBaseConf rssBaseConf,
      Set<Long> expectedBlockIds) {
    String allLocalPath = rssBaseConf.get(RssBaseConf.DATA_STORAGE_BASE_PATH);
    int indexReadLimit = rssBaseConf.get(RssBaseConf.RSS_STORAGE_INDEX_READ_LIMIT);
    String[] storageBasePaths = allLocalPath.split(",");

    long start = System.currentTimeMillis();
    if (storageBasePaths.length > 0) {
      for (String path : storageBasePaths) {
        prepareFilePath(appId, shuffleId, partitionId, partitionsPerServer, partitionNum, path);
      }
    } else {
      throw new RuntimeException("Can't get base path, please check rss.storage.localFile.basePaths.");
    }
    long prepareCost = System.currentTimeMillis() - start;
    start = System.currentTimeMillis();
    readAllIndexSegments(indexReadLimit, expectedBlockIds);
    LOG.debug("Prepare for appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId
        + "] cost " + prepareCost + " ms, read index cost " + (System.currentTimeMillis() - start) + " ms");
  }

  private void prepareFilePath(
      String appId,
      int shuffleId,
      int partitionId,
      int partitionsPerServer,
      int partitionNum,
      String storageBasePath) {
    String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPathWithRange(
            appId, shuffleId, partitionId, partitionsPerServer, partitionNum));

    File baseFolder = new File(fullShufflePath);
    try {
      if (!baseFolder.exists()) {
        // the partition doesn't exist in this base folder, skip
        return;
      }
    } catch (Exception e) {
      LOG.warn("Unexpected error when prepareFilePath", e);
    }
    File[] indexFiles;
    String failedGetIndexFileMsg = "No index file found in  " + storageBasePath;
    try {
      // get all index files
      indexFiles = baseFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX);
        }
      });
    } catch (Exception e) {
      throw new RuntimeException(failedGetIndexFileMsg, e);
    }

    if (indexFiles != null && indexFiles.length > 0) {
      for (File file : indexFiles) {
        String fileNamePrefix = getFileNamePrefix(file.getName());
        dataPathMap.put(fileNamePrefix,
            fullShufflePath + "/" + ShuffleStorageUtils.generateDataFileName(fileNamePrefix));
        indexPathMap.put(fileNamePrefix,
            fullShufflePath + "/" + ShuffleStorageUtils.generateIndexFileName(fileNamePrefix));
      }
    }
  }

  /**
   * Read all index files, and get all FileBasedShuffleSegment for every index file
   */
  private void readAllIndexSegments(int indexReadLimit, Set<Long> expectedBlockIds) {
    Set<Long> blockIds = Sets.newHashSet();
    for (Entry<String, String> entry : indexPathMap.entrySet()) {
      String path = entry.getKey();
      try {
        LOG.info("Read index file for: " + entry.getValue());
        try (LocalFileReader reader = createFileReader(entry.getValue())) {
          List<FileBasedShuffleSegment> segments = reader.readIndex(indexReadLimit);
          while (!segments.isEmpty()) {
            for (FileBasedShuffleSegment segment : segments) {
              blockIds.add(segment.getBlockId());
              allSegments.put(segment.getBlockId(), new FileReadSegment(segment, path));
            }
            segments = reader.readIndex(indexReadLimit);
          }
        }
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

  private String getFileNamePrefix(String fileName) {
    int point = fileName.lastIndexOf(".");
    return fileName.substring(0, point);
  }

  private LocalFileReader createFileReader(String path) throws Exception {
    return new LocalFileReader(path);
  }

  @Override
  public ShuffleDataResult getShuffleData(Set<Long> expectedBlockIds) {
    ShuffleDataResult shuffleDataResult = new ShuffleDataResult();
    List<BufferSegment> bufferSegments = Lists.newArrayList();
    if (!allSegments.isEmpty()) {
      long curSize = 0L;
      for (Long blockId : expectedBlockIds) {
        if (allSegments.containsKey(blockId)) {
          FileReadSegment fileSegment = allSegments.get(blockId);

          try {
            long start = System.currentTimeMillis();
            try (LocalFileReader reader = createFileReader(dataPathMap.get(fileSegment.getPath()))) {
              ByteBuffer data = reader.readData(fileSegment.getOffset(), fileSegment.getLength());
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
            }
            LOG.info("Read File segment: " + fileSegment.getPath() + ", offset["
                + fileSegment.getOffset() + "], length[" + fileSegment.getLength()
                + "], cost:" + (System.currentTimeMillis() - start) + " ms, for appId[" + appId
                + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]");
            readDataTime.addAndGet(System.currentTimeMillis() - start);
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
}
