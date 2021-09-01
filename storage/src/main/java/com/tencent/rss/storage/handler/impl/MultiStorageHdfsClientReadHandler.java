package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.tencent.rss.common.util.Constants;
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

    String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getUploadShuffleDataPath(appId, shuffleId, partitionId));
    init(fullShufflePath);
    String combinePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getCombineDataPath(appId, shuffleId));
    init(combinePath);
    readAllIndexSegments();
  }

  @Override
  protected void init(String fullShufflePath) {
    FileSystem fs;
    Path baseFolder = new Path(fullShufflePath);
    try {
      fs = ShuffleStorageUtils.getFileSystemForPath(baseFolder, hadoopConf);
    } catch (IOException ioe) {
      LOG.warn("Can't get FileSystem for " + baseFolder);
      return;
    }
    FileStatus[] indexFiles;
    String failedGetIndexFileMsg = "Can't list index file in  " + baseFolder;

    try {
      indexFiles = fs.listStatus(baseFolder,
          file -> file.getName().endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX));
    } catch (Exception e) {
      LOG.warn(failedGetIndexFileMsg, e);
      return;
    }

    if (indexFiles == null || indexFiles.length == 0) {
      LOG.warn(failedGetIndexFileMsg);
      return;
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
        HdfsFileReader reader = entry.getValue();
        ShuffleIndexHeader header = reader.readHeader();
        long start = System.currentTimeMillis();
        List<ShuffleIndexHeader.Entry> indexes = header.getIndexes();

        int limit = indexReadLimit;
        int offset = header.getHeaderLen();
        long lastPos = 0;
        for (ShuffleIndexHeader.Entry indexEntry : indexes) {
          long ts = reader.getOffset();
          if (indexEntry.getPartitionId() != partitionId) {
            lastPos += indexEntry.getPartitionDataLength();
            offset += indexEntry.partitionIndexLength;
            continue;
          }
          reader.seek(offset);
          long length = indexEntry.getPartitionIndexLength() / FileBasedShuffleSegment.SEGMENT_SIZE;
          int dataSize = 0;
          while (length > 0) {
            if (limit > length) {
              limit = (int) length;
              length = 0;
            } else {
              length = length - limit;
            }
            int segmentSize = 0;
            List<FileBasedShuffleSegment> segments = reader.readIndex(limit);
            for (FileBasedShuffleSegment segment : segments) {
              dataSize += segment.getLength();
              segmentSize += FileBasedShuffleSegment.SEGMENT_SIZE;
              if (dataSize > readBufferSize) {
                indexSegments.add(new MultiStorageFileSegment(path, offset, segmentSize, lastPos));
                offset += segmentSize;
                dataSize = 0;
                segmentSize = 0;
              }
            }
            if (dataSize > 0) {
              indexSegments.add(new MultiStorageFileSegment(path, offset, segmentSize, lastPos));
              offset += segmentSize;
            }
          }
          lastPos += indexEntry.getPartitionDataLength();
        }
        readIndexTime.addAndGet((System.currentTimeMillis() - start));
      } catch (Exception e) {
        LOG.warn("Can't read index segments for " + path, e);
      }
    }
  }

  @Override
  protected List<FileBasedShuffleSegment> getDataSegments(int dataSegmentIndex) {
    List<FileBasedShuffleSegment> segments = Lists.newArrayList();
    String path = "";
    if (indexSegments.size() > dataSegmentIndex) {
      try {
        int size = 0;
        MultiStorageFileSegment indexSegment = (MultiStorageFileSegment)indexSegments.get(dataSegmentIndex);
        path = indexSegment.getPath();
        HdfsFileReader reader = indexReaderMap.get(path);
        reader.seek(indexSegment.getOffset());
        FileBasedShuffleSegment segment = reader.readIndex();
        while (segment != null) {
          segment.setOffset(segment.getOffset() + indexSegment.getLastPos());
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

  class MultiStorageFileSegment extends FileSegment {
    private final long lastPos;

    MultiStorageFileSegment(String path, long offset, int length, long lastPos) {
      super(path, offset, length);
      this.lastPos = lastPos;
    }

    public long getLastPos() {
      return lastPos;
    }
  }
}
