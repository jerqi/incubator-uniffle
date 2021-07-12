package com.tencent.rss.storage.handler.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.handler.api.ShuffleUploadHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.ShuffleUploadResult;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Handler to upload local files to hdfs, it has two mode combine and not combine,
 *  upload all files to hdfs as one file, if it is combine mode,
 *  upload files one by one to remote storage as separate file, if it is not.
 *
 */
public class HdfsShuffleUploadHandler implements ShuffleUploadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ShuffleUploadHandler.class);

  private final String baseHdfsPath;
  private final Configuration hadoopConf;
  private final String hdfsFilePrefixBase;
  private final FileSystem fileSystem;
  private final int buffSize;
  private final boolean combineUpload;

  public HdfsShuffleUploadHandler(
      String baseHdfsPath,
      Configuration hadoopConf,
      String hdfsFilePrefixBase,
      int buffSize,
      boolean combineUpload) throws IOException, IllegalStateException {
    this.baseHdfsPath = baseHdfsPath;
    this.hadoopConf = hadoopConf;
    this.hdfsFilePrefixBase = hdfsFilePrefixBase + "-" + System.currentTimeMillis();
    this.fileSystem = getFileSystem();
    this.buffSize = buffSize;
    this.combineUpload = combineUpload;
  }

  // Upload data files and index files to remote storage by reading files one by one and
  // writing to the dfs stream using best-effort strategy to upload files so the result
  // may be part of the files.We do not merge file locally and then upload for there is
  // no obvious speed advantage considering the merge time and potential spindle or starving risk.
  public ShuffleUploadResult upload(
      List<File> dataFiles,
      List<File> indexFiles,
      List<Integer> partitions) {
    ShuffleUploadResult shuffleUploadResult;
    if (combineUpload) {
      shuffleUploadResult = uploadInternal(dataFiles, indexFiles, partitions);
    } else {
      List<ShuffleUploadResult> results = Lists.newLinkedList();
      for (int i = 0; i < partitions.size(); ++i) {
        ShuffleUploadResult cur = uploadInternal(
            dataFiles.subList(i, i + 1),
            indexFiles.subList(i, i + 1),
            partitions.subList(i, i + 1));
        if (cur != null) {
          results.add(cur);
        }
      }
      shuffleUploadResult =  ShuffleUploadResult.merge(results);
    }
    return shuffleUploadResult;
  }

  private String generateDataFileName(int partition) {
    if (combineUpload) {
      return ShuffleStorageUtils.generateDataFileName(
          "combine/" + hdfsFilePrefixBase + "-" + partition);
    } else {
      return ShuffleStorageUtils.generateDataFileName(
          partition + "/" + hdfsFilePrefixBase + "-" + partition);
    }
  }

  private String generateIndexFileName(int partition) {
    if (combineUpload) {
      return ShuffleStorageUtils.generateIndexFileName(
          "combine/" + hdfsFilePrefixBase + "-" + partition);
    } else {
      return ShuffleStorageUtils.generateIndexFileName(
          partition + "/" + hdfsFilePrefixBase + "-" + partition);
    }
  }

  // Use best-effort strategy to upload files one by one using the sequence of the partitions,
  // break the upload loop once encounter error and return the upload result.
  @VisibleForTesting
  ShuffleUploadResult uploadInternal(
      List<File> dataFiles,
      List<File> indexFiles,
      List<Integer> partitions) {
    // upload data files
    List<Long> fileSize = Lists.newLinkedList();

    String dataFileName = generateDataFileName(partitions.get(0));
    try (FSDataOutputStream dataStream = createOutputStream(dataFileName)) {
      for (File file : dataFiles) {
        if (!file.exists() || file.length() == 0) {
          LOG.error("Fail to upload data file {}, for it do not exist or length is 0", file.getAbsolutePath());
          break;
        }

        try {
          long sz = ShuffleStorageUtils.uploadFile(file, dataStream, buffSize);
          if (sz == 0) {
            LOG.error("Fail to upload data file {} upload size is 0", file.getAbsolutePath());
            break;
          }
          fileSize.add(sz);
        } catch (IOException e) {
          LOG.error("Fail to upload data file {}, for {}", file.getAbsolutePath(), e.getMessage());
          break;
        }
      }
    } catch (IOException | IllegalStateException e) {
      LOG.error("Fail to create data output stream {}, {}", dataFileName, e.getMessage());
      return null;
    }

    // upload index files
    int num = 0;
    String indexFileName = generateIndexFileName(partitions.get(0));
    if (fileSize.size() == 0) {
      LOG.error("No data file uploaded no need to upload index file {}", indexFileName);
      return null;
    }

    try (FSDataOutputStream indexStream = createOutputStream(indexFileName)) {
      try {
        writeIndexHeader(fileSize.size(), partitions, indexFiles, indexStream);
      } catch (IOException e) {
        LOG.error("Fail to write header to index output stream {}, {}", indexFileName, ExceptionUtils.getStackTrace(e));
        return null;
      }

      for (File file : indexFiles) {
        if (!file.exists() || file.length() == 0) {
          LOG.error("Fail to upload index file {}, for it do not exist or length is 0", file.getAbsolutePath());
          continue;
        }

        try {
          ShuffleStorageUtils.uploadFile(file, indexStream, buffSize);
          ++num;
        } catch (IOException e) {
          LOG.error("Fail to upload index file {}, for {}", file.getAbsolutePath(), ExceptionUtils.getStackTrace(e));
          break;
        }
      }

    } catch (IOException | IllegalStateException e) {
      LOG.error("Fail to create index output stream {}, {}", indexFileName, ExceptionUtils.getStackTrace(e));
      return null;
    }

    if (num == 0) {
      return null;
    } else {
      long sz = fileSize.subList(0, num).stream().reduce(0L, Long::sum);
      List<Integer> p = partitions.subList(0, num);
      return new ShuffleUploadResult(sz, p);
    }
  }

  // The partition files may be more than 10K, which is not suitable to list entire shuffle dir,
  // so we upload files to $BASE/$APPID/$SHUFFLEID/combine if combineUpload is true and upload
  // files to $BASE/$APPID/$SHUFFLEID/ if combineUpload is false.
  // The file num in $SHUFFLEID/combine is limit to disk size of the shuffle server and the client
  // can list the entire dir and find the target files. The file num in $SHUFFLEID/ may be
  // more than 10K, so we create subdir in it (eg, $SHUFFLEID/$PARTITION_ID/) and the client can list
  // the accurate dir and find the target files (eg, $SHUFFLEID/$PARTITION_ID/$SERVER_ID-$TS-$PARTITION_ID.data).
  @VisibleForTesting
  String getOrCreateTargetPath(int partitionId) {
    String targetDir;
    if (combineUpload) {
      targetDir = ShuffleStorageUtils.getFullShuffleDataFolder(baseHdfsPath, "combine");
    } else {
      targetDir = ShuffleStorageUtils.getFullShuffleDataFolder(baseHdfsPath, String.valueOf(partitionId));
    }

    if (createDirIfNotExist(targetDir)) {
      return targetDir;
    } else {
      return null;
    }
  }

  private boolean createDirIfNotExist(String pathString) {
    try {
      ShuffleStorageUtils.createDirIfNotExist(fileSystem, pathString);
      return true;
    } catch (IOException ioe) {
      LOG.error("Fail to create {}, {} tring to retry", pathString, ioe.getMessage());
    }

    try {
      ShuffleStorageUtils.createDirIfNotExist(fileSystem, pathString);
      return true;
    } catch (IOException ioe) {
      LOG.error("Fail to retry to create {}, {}", pathString, ioe.getMessage());
    }

    return false;
  }

  private FileSystem getFileSystem() throws IOException, IllegalStateException {
    Path path = new Path(baseHdfsPath);
    FileSystem fileSystem = null;

    try {
      fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);
      if (!fileSystem.exists(path)) {
        fileSystem.mkdirs(path);
      }
    } catch (IOException ioe) {
      // if folder exist, ignore the exception
      if (!fileSystem.exists(path)) {
        LOG.error("Can't create shuffle folder:" + baseHdfsPath, ioe);
        throw ioe;
      }
    }

    return fileSystem;
  }

  private FSDataOutputStream createOutputStream(String fileName) throws IOException, IllegalStateException {
    Path path = new Path(baseHdfsPath, fileName);
    if (fileSystem.isFile(path)) {
      String msg = path + " exists!";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    } else if (fileSystem.isDirectory(path)) {
      String msg = path + " is a directory!";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    } else {
      return fileSystem.create(path);
    }
  }

  // index file header is PartitionNum | [(PartitionId | PartitionFilexLength), ] | CRC
  @VisibleForTesting
  void writeIndexHeader(
      int partitionNum,
      List<Integer> partitions,
      List<File> files,
      FSDataOutputStream stream) throws IOException {
    int len = getIndexFileHeaderLen(partitionNum);
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.putInt(partitionNum);
    for (int i = 0; i < partitionNum; ++i) {
      buf.putInt(partitions.get(i));
      buf.putLong(files.get(i).length());
    }


    buf.flip();
    long crc = ChecksumUtils.getCrc32(buf);
    // continue to write crc
    buf.position(buf.limit());
    buf.limit(buf.capacity());
    buf.putLong(crc);

    buf.flip();
    stream.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
    if (stream.getPos() != (long) len) {
      throw new IOException("Fail to write index header");
    }
  }

  public String getHdfsFilePrefixBase() {
    return hdfsFilePrefixBase;
  }

  // index file header is $PartitionNum | [($PartitionId | $PartitionFilexLength), ] | $CRC
  public static int getIndexFileHeaderLen(int partitionNum) {
    return 4 + (4 + 8) * partitionNum + 8;
  }

  public String getBaseHdfsPath() {
    return baseHdfsPath;
  }
}
