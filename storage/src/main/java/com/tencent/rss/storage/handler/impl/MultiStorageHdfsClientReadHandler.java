package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.api.ShuffleReader;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.IOException;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
      Configuration hadoopConf) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.storageBasePath = storageBasePath;
    this.hadoopConf = hadoopConf;

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

  @Override
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

  @Override
  protected void readAllIndexSegments() {
    for (Entry<String, HdfsFileReader> entry : indexReaderMap.entrySet()) {
      String path = entry.getKey();
      try {
        LOG.info("Read index file for shuffleId[" + shuffleId + "], partitionId[" + partitionId + "] with " + path);
        //int limit = indexReadLimit;
        HdfsFileReader reader = entry.getValue();
        ShuffleIndexHeader header = reader.readHeader();
        long start = System.currentTimeMillis();
        Queue<ShuffleIndexHeader.Entry> indexes = header.getIndexes();
        long lastPos = 0;

        while (!indexes.isEmpty()) {
          ShuffleIndexHeader.Entry indexEntry = indexes.poll();

          if (indexEntry.getKey() != partitionId) {
            lastPos = indexEntry.getValue();
            continue;
          }

          int length = (int) (indexEntry.getValue() / FileBasedShuffleSegment.SEGMENT_SIZE);
          if (length <= 0) {
            break;
          }

          long  payloadOffset = header.getHeaderLen() + lastPos;
          reader.seek(payloadOffset);
          int limit = Math.min(length, indexReadLimit);
          List<FileBasedShuffleSegment> segments = reader.readIndex(limit);

          int dataSize = 0;
          int segmentSize = 0;
          long offset = payloadOffset;
          int sz = segments.size();
          while (!segments.isEmpty()) {
            for (FileBasedShuffleSegment segment : segments) {
              dataSize += segment.getLength();
              segmentSize += FileBasedShuffleSegment.SEGMENT_SIZE;
              if (dataSize > readBufferSize) {
                indexSegments.add(new FileSegment(path, offset, segmentSize));
                offset += segmentSize;
                dataSize = 0;
                segmentSize = 0;
              }
            }
            limit = Math.min(length - sz, indexReadLimit);

            if (limit > 0) {
              segments = reader.readIndex(limit);
            } else {
              break;
            }
          }

          if (dataSize > 0) {
            indexSegments.add(new FileSegment(path, offset, segmentSize));
          }

          break;
        }


        readIndexTime.addAndGet((System.currentTimeMillis() - start));

      } catch (Exception e) {
        LOG.warn("Can't read index segments for " + path, e);
      }
    }
  }

  protected List<FileBasedShuffleSegment> getDataSegments(int dataSegmentIndex) {
    List<FileBasedShuffleSegment> segments = Lists.newArrayList();
    String path = "";
    if (indexSegments.size() > dataSegmentIndex) {
      try {
        int size = 0;
        FileSegment indexSegment = indexSegments.get(dataSegmentIndex);
        path = indexSegment.getPath();
        HdfsFileReader reader = indexReaderMap.get(path);
        reader.seek(indexSegment.getOffset());
        FileBasedShuffleSegment segment = reader.readIndex();
        while (segment != null) {
          segments.add(segment);
          size += FileBasedShuffleSegment.SEGMENT_SIZE;
          if (size >= indexSegment.getLength()) {
            break;
          } else {
            segment = reader.readIndex();
          }
        }
      } catch (Exception e) {
        String msg = "Can't read index segments for " + path;
        LOG.warn(msg);
        throw new RuntimeException(msg, e);
      }
    }
    return segments;
  }
}
