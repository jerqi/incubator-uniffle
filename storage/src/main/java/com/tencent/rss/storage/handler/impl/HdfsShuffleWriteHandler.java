package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
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
  private HdfsFileWriter dataWriter;
  private HdfsFileWriter indexWriter;
  private boolean isClosed = false;

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
    createWriters();
  }

//  @Override
//  public synchronized void write(
//      List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException {
//    String dataFileName = ShuffleStorageUtils.generateDataFileName(fileNamePrefix);
//    String indexFileName = ShuffleStorageUtils.generateIndexFileName(fileNamePrefix);
//    long writeSize = shuffleBlocks.stream().mapToLong(ShufflePartitionedBlock::size).sum();
//
//    long startTimeOuter = System.currentTimeMillis();
//    try (HdfsFileWriter dataWriter = createWriter(dataFileName);
//        HdfsFileWriter indexWriter = createWriter(indexFileName)) {
//
//      long startTimeInner = System.currentTimeMillis();
//      for (ShufflePartitionedBlock block : shuffleBlocks) {
//        LOG.debug("Write data " + block);
//        long blockId = block.getBlockId();
//        long crc = block.getCrc();
//        long startOffset = dataWriter.nextOffset();
//        dataWriter.writeData(block.getData());
//
//        FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
//            blockId, startOffset, block.getLength(), block.getUncompressLength(), crc);
//        LOG.debug("Write index " + segment);
//        indexWriter.writeIndex(segment);
//      }
//      LOG.debug(
//          "Write handler write {} blocks {} bytes for {} ms without file open close",
//          shuffleBlocks.size(),
//          writeSize,
//          (System.currentTimeMillis() - startTimeInner));
//    }
//    LOG.debug(
//        "Write handler write {} blocks {} bytes for {} ms with file open close",
//        shuffleBlocks.size(),
//        writeSize,
//        (System.currentTimeMillis() - startTimeOuter));
//  }

  @Override
  public synchronized void write(
      List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException {
    if (isClosed) {
      createWriters();
    }
    final long writeSize = shuffleBlocks.stream().mapToLong(ShufflePartitionedBlock::size).sum();
    final long start = System.currentTimeMillis();
    for (ShufflePartitionedBlock block : shuffleBlocks) {
      LOG.debug("Write data " + block);
      long blockId = block.getBlockId();
      long crc = block.getCrc();
      long startOffset = dataWriter.nextOffset();
      dataWriter.writeData(block.getData());

      FileBasedShuffleSegment segment = new FileBasedShuffleSegment(
          blockId, startOffset, block.getLength(), block.getUncompressLength(), crc);
      LOG.debug("Write index " + segment);
      indexWriter.writeIndex(segment);
    }
    dataWriter.flush();
    indexWriter.flush();
    LOG.debug(
        "Write handler write {} blocks {} bytes for {} ms ",
        shuffleBlocks.size(),
        writeSize,
        (System.currentTimeMillis() - start));
  }

//  private HdfsFileWriter createWriter(String fileName) throws IOException, IllegalStateException {
//    Path path = new Path(basePath, fileName);
//    HdfsFileWriter writer = new HdfsFileWriter(path, hadoopConf);
//    writer.createStream();
//    return writer;
//  }

  @Override
  public synchronized boolean close() {
    if (!isClosed) {
      if (dataWriter != null) {
        try {
          dataWriter.close();
        } catch (IOException ioe) {
          LOG.error("Fail to close data file in " + basePath);
          return false;
        }
      }
      if (indexWriter != null) {
        try {
          indexWriter.close();
        } catch (IOException ioe) {
          LOG.error("Fail to close index file in " + basePath);
          return false;
        }
      }
      isClosed = true;
    }
    return true;
  }

  private void createWriters() throws IOException, IllegalStateException {
    String dataFileName = ShuffleStorageUtils.generateDataFileName(fileNamePrefix);
    String indexFileName = ShuffleStorageUtils.generateIndexFileName(fileNamePrefix);
    Path dataFilePath = new Path(basePath, dataFileName);
    Path indexFilePath = new Path(basePath, indexFileName);
    dataWriter = new HdfsFileWriter(dataFilePath, hadoopConf);
    indexWriter = new HdfsFileWriter(indexFilePath, hadoopConf);
    isClosed = false;
  }
}
