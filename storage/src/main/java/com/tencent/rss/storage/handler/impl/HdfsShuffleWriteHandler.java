package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
  private Lock writeLock = new ReentrantLock();

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
    this.basePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition));
    initialize();
  }

  private void initialize() throws IOException, IllegalStateException {
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
  public void write(
      List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException {
    final long start = System.currentTimeMillis();
    final long writeSize = shuffleBlocks.stream().mapToLong(ShufflePartitionedBlock::size).sum();
    String dataFileName = ShuffleStorageUtils.generateDataFileName(fileNamePrefix);
    String indexFileName = ShuffleStorageUtils.generateIndexFileName(fileNamePrefix);
    writeLock.lock();
    try {
      try (HdfsFileWriter dataWriter = createWriter(dataFileName);
          HdfsFileWriter indexWriter = createWriter(indexFileName)) {
        final long ss = System.currentTimeMillis();
        for (ShufflePartitionedBlock block : shuffleBlocks) {
          long blockId = block.getBlockId();
          long crc = block.getCrc();
          long startOffset = dataWriter.nextOffset();
          dataWriter.writeData(block.getData());

          FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
              blockId, startOffset, block.getLength(), block.getUncompressLength(), crc);
          indexWriter.writeIndex(segment);
        }
        LOG.debug(
            "Write handler inside cost {} ms for {}",
            (System.currentTimeMillis() - ss),
            fileNamePrefix);
      }
    } finally {
      writeLock.unlock();
    }
    LOG.debug(
        "Write handler outside write {} blocks {} bytes cost {} ms for {}",
        shuffleBlocks.size(),
        writeSize,
        (System.currentTimeMillis() - start),
        fileNamePrefix);
  }

  private HdfsFileWriter createWriter(String fileName) throws IOException, IllegalStateException {
    Path path = new Path(basePath, fileName);
    HdfsFileWriter writer = new HdfsFileWriter(path, hadoopConf);
    return writer;
  }
}
