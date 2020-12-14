package com.tencent.rss.storage;

import com.tencent.rss.common.util.Constants;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleStorageUtils {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleStorageUtils.class);

    public static FileSystem getFileSystemForPath(Path path, Configuration conf) throws IOException {
        // For local file systems, return the raw local file system, such calls to flush()
        // actually flushes the stream.
        try {
            FileSystem fs = path.getFileSystem(conf);
            if (fs instanceof LocalFileSystem) {
                logger.info("{} is local file system", path);
                return ((LocalFileSystem) fs).getRawFileSystem();
            }
            return fs;
        } catch (IOException e) {
            logger.error("Fail to get filesystem of {}", path);
            throw e;
        }
    }

    public static String generateDataFileName(String fileNamePrefix) {
        return fileNamePrefix + Constants.SHUFFLE_DATA_FILE_SUFFIX;
    }

    public static String generateIndexFileName(String fileNamePrefix) {
        return fileNamePrefix + Constants.SHUFFLE_INDEX_FILE_SUFFIX;
    }
}
