package com.tencent.rss.storage;

import com.tencent.rss.common.ShufflePartitionedBlock;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedShuffleWriteHandler implements ShuffleStorageWriteHandler {

    private static final Logger logger = LoggerFactory.getLogger(FileBasedShuffleWriteHandler.class);
    private Configuration hadoopConf;
    private String basePath;

    public FileBasedShuffleWriteHandler(
            String basePath, Configuration hadoopConf) throws IOException, IllegalStateException {
        this.basePath = basePath;
        this.hadoopConf = hadoopConf;
        createBasePath();
    }

    private void createBasePath() throws IOException, IllegalStateException {
        Path path = new Path(basePath);
        FileSystem fileSystem = ShuffleStorageUtils.getFileSystemForPath(path, hadoopConf);
        if (fileSystem.exists(path)) {
            String msg = path + " is already exist.";
            logger.error(msg);
            throw new IllegalStateException(msg);
        }
        fileSystem.mkdirs(path);
    }

    public void write(List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException {
        String dataFileName = generateFileName("data");
        String indexFileName = generateFileName("index");
        try (
                FileBasedShuffleWriter dataWriter = createWriter(dataFileName);
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

    private String generateFileName(String ref) {
        return ref;
    }

    private FileBasedShuffleWriter createWriter(String fileName) throws IOException, IllegalStateException {
        Path path = new Path(basePath, fileName);
        FileBasedShuffleWriter writer = new FileBasedShuffleWriter(path, hadoopConf);
        writer.createStream();
        return writer;
    }

}
