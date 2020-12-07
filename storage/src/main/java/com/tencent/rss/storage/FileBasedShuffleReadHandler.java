package com.tencent.rss.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedShuffleReadHandler implements ShuffleStorageReaderHandler<FileBasedShuffleSegment>, Closeable {

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
    public byte[] readData(FileBasedShuffleSegment segment)
            throws IOException, IllegalStateException {
        byte[] data = dataReader.readData(segment);
        return data;
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
