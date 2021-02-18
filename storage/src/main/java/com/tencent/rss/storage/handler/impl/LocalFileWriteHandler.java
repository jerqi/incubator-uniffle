package com.tencent.rss.storage.handler.impl;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileWriteHandler implements ShuffleWriteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LocalFileWriteHandler.class);

  private String fileNamePrefix;
  private String basePath;

  public LocalFileWriteHandler(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      String[] storageBasePaths,
      String fileNamePrefix) {
    this.fileNamePrefix = fileNamePrefix;
    String storageBasePath = pickBasePath(storageBasePaths);
    this.basePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPath(appId, shuffleId, startPartition, endPartition));
    createBasePath();
  }

  private void createBasePath() {
    File baseFolder = new File(basePath);
    // check if shuffle folder exist
    if (!baseFolder.exists()) {
      try {
        // try to create folder, it may be created by other Shuffle Server
        baseFolder.mkdirs();
      } catch (Exception e) {
        // if folder exist, ignore the exception
        if (!baseFolder.exists()) {
          LOG.error("Can't create shuffle folder:" + basePath, e);
          throw e;
        }
      }
    }
  }

  // pick basepath by random
  private String pickBasePath(String[] storageBasePaths) {
    if (storageBasePaths == null || storageBasePaths.length == 0) {
      throw new RuntimeException("Base path can't be empty, please check rss.storage.localFile.basePaths");
    }
    Random random = new Random();
    return storageBasePaths[random.nextInt(storageBasePaths.length)];
  }

  @Override
  public void write(List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException {
    long accessTime = System.currentTimeMillis();
    String dataFileName = ShuffleStorageUtils.generateDataFileName(fileNamePrefix);
    String indexFileName = ShuffleStorageUtils.generateIndexFileName(fileNamePrefix);
    long writeSize = shuffleBlocks.stream().mapToLong(ShufflePartitionedBlock::size).sum();

    try (LocalFileWriter dataWriter = createWriter(dataFileName);
        LocalFileWriter indexWriter = createWriter(indexFileName)) {

      long startTime = System.currentTimeMillis();
      for (ShufflePartitionedBlock block : shuffleBlocks) {
        LOG.debug("Write data " + block);
        long blockId = block.getBlockId();
        long crc = block.getCrc();
        long startOffset = dataWriter.nextOffset();
        dataWriter.writeData(block.getData());

        long endOffset = dataWriter.nextOffset();
        long len = endOffset - startOffset;

        FileBasedShuffleSegment segment = new FileBasedShuffleSegment(blockId, startOffset, len, crc);
        LOG.debug("Write index " + segment);
        indexWriter.writeIndex(segment);
      }
      LOG.debug(
          "Write handler write {} blocks {} mb for {} ms without file open close",
          shuffleBlocks.size(),
          writeSize,
          (System.currentTimeMillis() - startTime));
    }
    LOG.debug(
        "Write handler write {} blocks {} mb for {} ms with file open close",
        shuffleBlocks.size(),
        writeSize,
        (System.currentTimeMillis() - accessTime));
  }

  private LocalFileWriter createWriter(String fileName) throws IOException, IllegalStateException {
    File file = new File(basePath, fileName);
    return new LocalFileWriter(file);
  }

  @VisibleForTesting
  protected String getBasePath() {
    return basePath;
  }

}
