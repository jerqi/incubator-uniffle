package com.tencent.rss.storage.util;

import com.google.common.collect.Lists;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.impl.FileReadSegment;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleStorageUtils {

  static final String HDFS_PATH_SEPARATOR = "/";
  static final String HDFS_DIRNAME_SEPARATOR = "-";
  private static final Logger logger = LoggerFactory.getLogger(ShuffleStorageUtils.class);

  public static FileSystem getFileSystemForPath(Path path, Configuration conf) throws IOException {
    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    try {
      FileSystem fs = path.getFileSystem(conf);
      if (fs instanceof LocalFileSystem) {
        logger.debug("{} is local file system", path);
        return ((LocalFileSystem) fs).getRawFileSystem();
      }
      return fs;
    } catch (IOException e) {
      logger.error("Fail to get filesystem of {}", path);
      throw e;
    }
  }

  public static String generateDataFileName(String fileNamePrefix) {
    return fileNamePrefix + Constants.SHUFFLE_DATA_FILE_SUFFIX;
  }

  public static String generateIndexFileName(String fileNamePrefix) {
    return fileNamePrefix + Constants.SHUFFLE_INDEX_FILE_SUFFIX;
  }

  public static List<FileReadSegment> mergeSegments(
      String path, List<FileBasedShuffleSegment> segments, int readBufferSize) {
    List<FileReadSegment> fileReadSegments = Lists.newArrayList();
    if (segments != null && !segments.isEmpty()) {
      if (segments.size() == 1) {
        List<BufferSegment> bufferSegments = Lists.newArrayList();
        bufferSegments.add(new BufferSegment(segments.get(0).getBlockId(), 0,
            segments.get(0).getLength(), segments.get(0).getUncompressLength(), segments.get(0).getCrc()));
        fileReadSegments.add(new FileReadSegment(
            path, segments.get(0).getOffset(), segments.get(0).getLength(), bufferSegments));
      } else {
        Collections.sort(segments);
        long start = -1;
        long lastestPosition = -1;
        long skipThreshold = readBufferSize / 2;
        long lastPosition = Long.MAX_VALUE;
        List<BufferSegment> bufferSegments = Lists.newArrayList();
        for (FileBasedShuffleSegment segment : segments) {
          // check if there has expected skip range, eg, [20, 100], [1000, 1001] and the skip range is [101, 999]
          if (start > -1 && segment.getOffset() - lastPosition > skipThreshold) {
            fileReadSegments.add(new FileReadSegment(
                path, start, (int) (lastPosition - start), bufferSegments));
            start = -1;
          }
          // previous FileBasedShuffleSegment are merged, start new merge process
          if (start == -1) {
            bufferSegments = Lists.newArrayList();
            start = segment.getOffset();
          }
          lastestPosition = segment.getOffset() + segment.getLength();
          bufferSegments.add(new BufferSegment(segment.getBlockId(),
              segment.getOffset() - start, segment.getLength(),
              segment.getUncompressLength(), segment.getCrc()));
          if (lastestPosition - start >= readBufferSize) {
            fileReadSegments.add(new FileReadSegment(
                path, start, (int) (lastestPosition - start), bufferSegments));
            start = -1;
          }
          lastPosition = lastestPosition;
        }
        if (start > -1) {
          fileReadSegments.add(new FileReadSegment(path, start, (int) (lastPosition - start), bufferSegments));
        }
      }
    }
    return fileReadSegments;
  }

  public static String getShuffleDataPath(String appId, int shuffleId, int start, int end) {
    return String.join(
        HDFS_PATH_SEPARATOR,
        appId,
        String.valueOf(shuffleId),
        String.join(HDFS_DIRNAME_SEPARATOR, String.valueOf(start), String.valueOf(end)));
  }

  public static String getFullShuffleDataFolder(String basePath, String subPath) {
    return String.join(HDFS_PATH_SEPARATOR, basePath, subPath);
  }

  public static String getShuffleDataPathWithRange(
      String appId, int shuffleId, int partitionId,
      int partitionNumPerRange, int partitionNum) {
    int prNum = partitionNum % partitionNumPerRange == 0
        ? partitionNum / partitionNumPerRange : partitionNum / partitionNumPerRange + 1;
    for (int i = 0; i < prNum; i++) {
      int start = i * partitionNumPerRange;
      int end = (i + 1) * partitionNumPerRange - 1;
      if (partitionId >= start && partitionId <= end) {
        return getShuffleDataPath(appId, shuffleId, start, end);
      }
    }
    throw new RuntimeException("Can't generate ShuffleData Path for appId[" + appId + "], shuffleId["
        + shuffleId + "], partitionId[" + partitionId + "], partitionNumPerRange[" + partitionNumPerRange
        + "], partitionNum[" + partitionNum + "]");
  }

  public static int[] getPartitionRange(int partitionId, int partitionNumPerRange, int partitionNum) {
    int[] range = null;
    int prNum = partitionNum % partitionNumPerRange == 0
        ? partitionNum / partitionNumPerRange : partitionNum / partitionNumPerRange + 1;
    for (int i = 0; i < prNum; i++) {
      int start = i * partitionNumPerRange;
      int end = (i + 1) * partitionNumPerRange - 1;
      if (partitionId >= start && partitionId <= end) {
        range = new int[]{start, end};
        break;
      }
    }
    return range;
  }

  public static int getStorageIndex(int max, String appId, int shuffleId, int startPartition) {
    int code = appId.hashCode();
    if (code < 0) {
      code = code + 1;
    }
    int index = (code + shuffleId * 31 + startPartition * 31) % max;
    if (index < 0) {
      index = -index;
    }
    return index;
  }
}
