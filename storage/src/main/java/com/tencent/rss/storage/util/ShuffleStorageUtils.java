package com.tencent.rss.storage.util;

import com.google.common.collect.Lists;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.impl.DataFileSegment;
import com.tencent.rss.storage.handler.impl.FileSegment;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleStorageUtils {

  static final String HDFS_PATH_SEPARATOR = "/";
  static final String HDFS_DIRNAME_SEPARATOR = "-";
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleStorageUtils.class);

  private ShuffleStorageUtils() {
  }

  public static FileSystem getFileSystemForPath(Path path, Configuration conf) throws IOException {
    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    try {
      FileSystem fs = path.getFileSystem(conf);
      if (fs instanceof LocalFileSystem) {
        LOG.debug("{} is local file system", path);
        return ((LocalFileSystem) fs).getRawFileSystem();
      }
      return fs;
    } catch (IOException e) {
      LOG.error("Fail to get filesystem of {}", path);
      throw e;
    }
  }

  public static String generateDataFileName(String fileNamePrefix) {
    return fileNamePrefix + Constants.SHUFFLE_DATA_FILE_SUFFIX;
  }

  public static String generateIndexFileName(String fileNamePrefix) {
    return fileNamePrefix + Constants.SHUFFLE_INDEX_FILE_SUFFIX;
  }

  public static String generateAbsoluteFilePrefix(String base, String key, int partition, String id) {
    return String.join(
        HDFS_PATH_SEPARATOR,
        base,
        key,
        String.join(HDFS_DIRNAME_SEPARATOR, String.valueOf(partition), String.valueOf(partition)),
        id);
  }

  public static List<DataFileSegment> mergeSegments(
      String path, List<FileBasedShuffleSegment> segments, int readBufferSize) {
    List<DataFileSegment> dataFileSegments = Lists.newArrayList();
    if (segments != null && !segments.isEmpty()) {
      if (segments.size() == 1) {
        List<BufferSegment> bufferSegments = Lists.newArrayList();
        bufferSegments.add(new BufferSegment(segments.get(0).getBlockId(), 0,
            segments.get(0).getLength(), segments.get(0).getUncompressLength(), segments.get(0).getCrc(),
            segments.get(0).getTaskAttemptId()));
        dataFileSegments.add(new DataFileSegment(
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
            dataFileSegments.add(new DataFileSegment(
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
              segment.getUncompressLength(), segment.getCrc(), segment.getTaskAttemptId()));
          if (lastestPosition - start >= readBufferSize) {
            dataFileSegments.add(new DataFileSegment(
                path, start, (int) (lastestPosition - start), bufferSegments));
            start = -1;
          }
          lastPosition = lastestPosition;
        }
        if (start > -1) {
          dataFileSegments.add(new DataFileSegment(path, start, (int) (lastPosition - start), bufferSegments));
        }
      }
    }
    return dataFileSegments;
  }

  public static String getShuffleDataPath(String appId, int shuffleId) {
    return String.join(
        HDFS_PATH_SEPARATOR,
        appId,
        String.valueOf(shuffleId));
  }

  public static String getShuffleDataPath(String appId, int shuffleId, int start, int end) {
    return String.join(
        HDFS_PATH_SEPARATOR,
        appId,
        String.valueOf(shuffleId),
        String.join(HDFS_DIRNAME_SEPARATOR, String.valueOf(start), String.valueOf(end)));
  }

  public static String getUploadShuffleDataPath(String appId, int shuffleId, int partitionId) {
    return String.join(
        HDFS_PATH_SEPARATOR,
        appId,
        String.valueOf(shuffleId),
        String.valueOf(partitionId));
  }

  public static String getCombineDataPath(String appId, int shuffleId) {
    return String.join(
        HDFS_PATH_SEPARATOR,
        appId,
        String.valueOf(shuffleId),
        "combine");
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

  public static void createDirIfNotExist(FileSystem fileSystem, String pathString) throws IOException {
    Path path = new Path(pathString);
    try {
      if (!fileSystem.exists(path)) {
        fileSystem.mkdirs(path);
      }
    } catch (IOException ioe) {
      // if folder exist, ignore the exception
      if (!fileSystem.exists(path)) {
        LOG.error("Can't create shuffle folder {}, {}", pathString, ExceptionUtils.getStackTrace(ioe));
        throw ioe;
      }
    }
  }

  public static long uploadFile(
      File file, FSDataOutputStream fsDataOutputStream, int bufferSize) throws IOException {
    long start = fsDataOutputStream.getPos();
    try (FileInputStream inputStream = new FileInputStream(file)) {
      IOUtils.copyBytes(inputStream, fsDataOutputStream, bufferSize);
      return fsDataOutputStream.getPos() - start;
    } catch (IOException e) {
      LOG.error("Fail to upload file {}, {}", file.getAbsolutePath(), e);
      throw new IOException(e);
    }
  }
}
