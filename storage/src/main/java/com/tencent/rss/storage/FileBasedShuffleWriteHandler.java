package com.tencent.rss.storage;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.Constants;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedShuffleWriteHandler implements ShuffleStorageWriteHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedShuffleWriteHandler.class);
  private static final Logger LOG_RSS_INFO = LoggerFactory.getLogger(Constants.LOG4J_RSS_SHUFFLE_PREFIX);

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
          LOG.error("Can't create shuffle folder:" + basePath, ioe);
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
        LOG_RSS_INFO.info("Write index " + block);
        long blockId = block.getBlockId();
        long crc = block.getCrc();
        long startOffset = dataWriter.nextOffset();
        dataWriter.writeData(block.getData());

        long endOffset = dataWriter.nextOffset();
        long len = endOffset - startOffset;

        FileBasedShuffleSegment segment = new FileBasedShuffleSegment(startOffset, len, crc, blockId);
        LOG_RSS_INFO.info("Write index " + segment);
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
