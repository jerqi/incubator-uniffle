package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.utils.ShuffleStorageUtils;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsShuffleWriteHandler implements ShuffleWriteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsShuffleWriteHandler.class);

  private Configuration hadoopConf;
  private String basePath;
  private String fileNamePrefix;
  private long accessTime;

  public HdfsShuffleWriteHandler(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String storageBasePath,
      String fileNamePrefix,
      Configuration hadoopConf) throws IOException, IllegalStateException {
    this.hadoopConf = hadoopConf;
    this.fileNamePrefix = fileNamePrefix;
    this.accessTime = System.currentTimeMillis();
    this.basePath = RssUtils.getFullShuffleDataFolder(storageBasePath,
        RssUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition));
    createBasePath();
  }

  private void createBasePath() throws IOException, IllegalStateException {

    Path path = new Path(basePath);
    FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);
    // check if shuffle folder exist
    if (!fileSystem.exists(path)) {
      try {
        // try to create folder, it may be created by other Shuffle Server
        fileSystem.mkdirs(path);
      } catch (IOException ioe) {
        // if folder exist, ignore the exception
        if (!fileSystem.exists(path)) {
          LOG.error("Can't create shuffle folder:" + basePath, ioe);
          throw ioe;
        }
      }
    }
  }

  @Override
  public long getAccessTime() {
    return accessTime;
  }

  @Override
  public synchronized void write(
      List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException {
    accessTime = System.currentTimeMillis();
    String dataFileName = ShuffleStorageUtils.generateDataFileName(fileNamePrefix);
    String indexFileName = ShuffleStorageUtils.generateIndexFileName(fileNamePrefix);
    long writeSize = shuffleBlocks.stream().mapToLong(ShufflePartitionedBlock::size).sum();

    try (FileBasedShuffleWriter dataWriter = createWriter(dataFileName);
        FileBasedShuffleWriter indexWriter = createWriter(indexFileName)) {

      long startTime = System.currentTimeMillis();
      for (ShufflePartitionedBlock block : shuffleBlocks) {
        LOG.debug("Write data " + block);
        long blockId = block.getBlockId();
        long crc = block.getCrc();
        long startOffset = dataWriter.nextOffset();
        dataWriter.writeData(block.getData());

        long endOffset = dataWriter.nextOffset();
        long len = endOffset - startOffset;

        FileBasedShuffleSegment segment = new FileBasedShuffleSegment(startOffset, len, crc, blockId);
        LOG.debug("Write index " + segment);
        indexWriter.writeIndex(segment);
      }
      LOG.debug(
          "Write handler write {} blocks {} mb for {} ms without file open close",
          shuffleBlocks.size(),
          writeSize,
          (System.currentTimeMillis() - startTime) / 1000);
    }
    LOG.debug(
        "Write handler write {} blocks {} mb for {} ms with file open close",
        shuffleBlocks.size(),
        writeSize,
        (System.currentTimeMillis() - accessTime / 1000));
  }

  private FileBasedShuffleWriter createWriter(String fileName) throws IOException, IllegalStateException {
    Path path = new Path(basePath, fileName);
    FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, hadoopConf);
    writer.createStream();
    return writer;
  }

}
