package com.tencent.rss.storage;

import com.google.protobuf.ByteString;
import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedShuffleReadHandler implements ShuffleStorageReaderHandler, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(FileBasedShuffleReadHandler.class);
    private final int indexReadLimit;
    private final int dataReadLimit;
    private Configuration hadoopConf;
    private String basePath;
    private FileBasedShuffleReader dataReader;
    private FileBasedShuffleReader indexReader;

    public FileBasedShuffleReadHandler(
            String basePath,
            Configuration hadoopConf) throws IOException, IllegalStateException {
        this(
                basePath,
                hadoopConf,
                1024 * 1024, // the segments size would be 32MB
                1024 // 32K index and 1k blocks at most
        );
    }

    public FileBasedShuffleReadHandler(
            String basePath,
            Configuration hadoopConf,
            int indexReadLimit,
            int dataReadLimit) throws IOException, IllegalStateException {
        this.basePath = basePath;
        this.hadoopConf = hadoopConf;
        this.indexReadLimit = indexReadLimit;
        this.dataReadLimit = dataReadLimit;
        init();
    }

    @Override
    public List<FileBasedShuffleSegment> readIndex() throws IOException, IllegalStateException {
        int defaultLimit = indexReadLimit;
        return readIndex(defaultLimit);
    }

    @Override
    public List<FileBasedShuffleSegment> readIndex(int limit) throws IOException, IllegalStateException {
        List<FileBasedShuffleSegment> fileBasedShuffleSegments = indexReader.readIndex(limit);
        if (fileBasedShuffleSegments.isEmpty()) {
            return null;
        }

        return fileBasedShuffleSegments;
    }

    @Override
    public List<ShuffleBlock> readData() throws IOException, IllegalStateException {
        int defaultLimit = dataReadLimit;
        return readData(defaultLimit);
    }

    @Override
    public List<ShuffleBlock> readData(int limit) throws IOException, IllegalStateException {
        List<FileBasedShuffleSegment> fileBasedShuffleSegments = indexReader.readIndex(limit);
        if (fileBasedShuffleSegments.isEmpty()) {
            return null;
        }
        return readData(fileBasedShuffleSegments);
    }

    public List<ShuffleBlock> readData(List<FileBasedShuffleSegment> segments)
            throws IOException, IllegalStateException {
        List<ShuffleBlock> ret = new LinkedList<>();

        for (FileBasedShuffleSegment segment : segments) {
            byte[] data = dataReader.readData(segment);
            ShuffleBlock block = ShuffleBlock
                    .newBuilder()
                    .setBlockId(segment.getBlockId())
                    .setCrc(segment.getCrc())
                    .setData(ByteString.copyFrom(data)).build();
            ret.add(block);
        }

        return ret;
    }

    public int getIndexReadLimit() {
        return this.indexReadLimit;
    }

    public int getDataReadLimit() {
        return this.dataReadLimit;
    }

    @Override
    public synchronized void close() throws IOException {
        dataReader.close();
        indexReader.close();
    }

    private void init() throws IOException, IllegalStateException {
        dataReader = createReader(generateFileName("data"));
        indexReader = createReader(generateFileName("index"));
    }

    private String generateFileName(String ref) {
        return ref;
    }

    private FileBasedShuffleReader createReader(String fileName) throws IOException, IllegalStateException {
        Path path = new Path(basePath, fileName);
        FileBasedShuffleReader reader = new FileBasedShuffleReader(path, hadoopConf);
        reader.createStream();
        return reader;
    }

}
