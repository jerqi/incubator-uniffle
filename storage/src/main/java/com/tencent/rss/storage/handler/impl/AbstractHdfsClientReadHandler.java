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

public abstract class AbstractHdfsClientReadHandler extends AbstractFileClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHdfsClientReadHandler.class);
  protected int indexReadLimit;
  private Map<String, HdfsFileReader> dataReaderMap = Maps.newHashMap();
  protected Map<String, HdfsFileReader> indexReaderMap = Maps.newHashMap();
  protected Set<Long> expectedBlockIds = Sets.newHashSet();
  protected List<FileReadSegment> fileReadSegments = Lists.newArrayList();
  protected int partitionNumPerRange;
  protected int partitionNum;
  protected int readBufferSize;
  protected String storageBasePath;
  protected AtomicLong readIndexTime = new AtomicLong(0);
  protected AtomicLong readDataTime = new AtomicLong(0);
  protected Configuration hadoopConf;

  protected void init(String fullShufflePath) {
    FileSystem fs;
    Path baseFolder = new Path(fullShufflePath);
    try {
      fs = ShuffleStorageUtils.getFileSystemForPath(baseFolder, hadoopConf);
    } catch (IOException ioe) {
      throw new RuntimeException("Can't get FileSystem for " + baseFolder);
    }

    FileStatus[] indexFiles;
    String failedGetIndexFileMsg = "Can't list index file in  " + baseFolder;

    try {
      // get all index files
      indexFiles = fs.listStatus(baseFolder,
          file -> file.getName().endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX));
    } catch (Exception e) {
      LOG.error(failedGetIndexFileMsg, e);
      throw new RuntimeException(failedGetIndexFileMsg);
    }

    if (indexFiles == null || indexFiles.length == 0) {
      throw new RuntimeException(failedGetIndexFileMsg);
    }

    for (FileStatus status : indexFiles) {
      LOG.info("Find index file for shuffleId[" + shuffleId + "], partitionId["
          + partitionId + "] " + status.getPath());
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

  }

  private String getFileNamePrefix(String fileName) {
    int point = fileName.lastIndexOf(".");
    return fileName.substring(0, point);
  }

  /**
   * Read all index files, and get all FileBasedShuffleSegment for every index file
   */
  protected void readAllIndexSegments() {
    Set<Long> blockIds = Sets.newHashSet();
    for (Entry<String, HdfsFileReader> entry : indexReaderMap.entrySet()) {
      String path = entry.getKey();
      try {
        LOG.info("Read index file for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "] with " + path);
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
      throw new RuntimeException("Missing " + copy.size() + " blockIds, expected "
          + expectedBlockIds.size() + " blockIds");
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
              readBuffer = dataReaderMap.get(fileSegment.getPath())
                  .readData(fileSegment.getOffset(), fileSegment.getLength());
              long readCost = System.currentTimeMillis() - start;
              LOG.info("Read File segment: " + fileSegment.getPath() + ", offset["
                  + fileSegment.getOffset() + "], length[" + fileSegment.getLength()
                  + "], cost:" + readCost + " ms, for " + leftIds.size() + " blocks");
              readDataTime.addAndGet(readCost);
              bufferSegments.addAll(fileSegment.getBufferSegments());
              break;
            } catch (Exception e) {
              LOG.warn("Can't read data for " + fileSegment.getPath() + ", offset["
                  + fileSegment.getOffset() + "], length[" + fileSegment.getLength() + "]", e);
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
    LOG.info("HdfsClientReadHandler for appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId["
        + partitionId + "] cost " + readIndexTime.get() + " ms for read index and "
        + readDataTime.get() + " ms for read data");
  }

  private HdfsFileReader createHdfsReader(
      String folder, String fileName, Configuration hadoopConf) throws IOException, IllegalStateException {
    Path path = new Path(folder, fileName);
    HdfsFileReader reader = new HdfsFileReader(path, hadoopConf);
    return reader;
  }
}
