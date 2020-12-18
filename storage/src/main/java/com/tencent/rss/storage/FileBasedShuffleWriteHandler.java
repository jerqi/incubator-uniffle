package com.tencent.rss.storage;

import com.tencent.rss.common.ShufflePartitionedBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class FileBasedShuffleWriteHandler implements ShuffleStorageWriteHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedShuffleWriteHandler.class);
  private Configuration hadoopConf;
  private String basePath;
  private String fileNamePrefix;

  public FileBasedShuffleWriteHandler(
    String basePath, String fileNamePrefix, Configuration hadoopConf)
    throws IOException, IllegalStateException {
    this.basePath = basePath;
    this.hadoopConf = hadoopConf;
    this.fileNamePrefix = fileNamePrefix;
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
          LOGGER.error("Can't create shuffle folder:" + basePath, ioe);
          throw ioe;
        }
      }
    }
  }

  public void write(List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException {
    String dataFileName = ShuffleStorageUtils.generateDataFileName(fileNamePrefix);
    String indexFileName = ShuffleStorageUtils.generateIndexFileName(fileNamePrefix);
    try (FileBasedShuffleWriter dataWriter = createWriter(dataFileName);
         FileBasedShuffleWriter indexWriter = createWriter(indexFileName)) {

      for (ShufflePartitionedBlock block : shuffleBlocks) {
        long blockId = block.getBlockId();
        long crc = block.getCrc();
        long startOffset = dataWriter.nextOffset();
        dataWriter.writeData(block.getData());

        long endOffset = dataWriter.nextOffset();
        long len = endOffset - startOffset;

        FileBasedShuffleSegment segment = new FileBasedShuffleSegment(startOffset, len, crc, blockId);
        indexWriter.writeIndex(segment);
      }
    }

  }

  private FileBasedShuffleWriter createWriter(String fileName) throws IOException, IllegalStateException {
    Path path = new Path(basePath, fileName);
    FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, hadoopConf);
    writer.createStream();
    return writer;
  }

}
