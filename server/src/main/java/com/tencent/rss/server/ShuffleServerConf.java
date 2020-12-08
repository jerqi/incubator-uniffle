package com.tencent.rss.server;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.RssConf;
import com.tencent.rss.common.util.RssUtils;
import java.io.File;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleServerConf extends RssConf {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServerConf.class);

    private static final ConfigOption<Integer> SERVICE_PORT = ConfigOptions
            .key("rss.server.port")
            .intType()
            .noDefaultValue()
            .withDescription("Shuffle server service port");

    private static final ConfigOption<String> DATA_STORAGE_TYPE = ConfigOptions
            .key("rss.storage.type")
            .stringType()
            .noDefaultValue()
            .withDescription("Data storage for remote shuffle service");

    private static final ConfigOption<String> DATA_STORAGE_BASE_PATH = ConfigOptions
            .key("rss.data.storage.basePath")
            .stringType()
            .noDefaultValue()
            .withDescription("Common storage path for remote shuffle data");

    private static final ConfigOption<Integer> BUFFER_CAPACITY = ConfigOptions
            .key("rss.buffer.capacity")
            .intType()
            .noDefaultValue()
            .withDescription("Number of buffers in this server");

    private static final ConfigOption<Integer> BUFFER_SIZE = ConfigOptions
            .key("rss.buffer.size")
            .intType()
            .noDefaultValue()
            .withDescription("Size of each buffer in this server");

    public int getServerPort() {
        return getInteger(SERVICE_PORT);
    }

    public String getDataStorageType() {
        return getString(DATA_STORAGE_TYPE);
    }

    public String getDataStoragePath() {
        return getString(DATA_STORAGE_BASE_PATH);
    }

    public Integer getBufferCapacity() {
        return getInteger(BUFFER_CAPACITY);
    }

    public Integer getBufferSize() {
        return getInteger(BUFFER_SIZE);
    }

    public boolean loadConfFromFile(String fileName) {
        if (fileName == null) {
            String rssHome = System.getenv("RSS_HOME");
            if (rssHome == null) {
                logger.error("Both conf file and RSS_HOME env is null");
                return false;
            }
            fileName = rssHome + "/server.conf";
        }

        logger.info("Load config from {}", fileName);
        File file = new File(fileName);
        if (!file.isFile()) {
            String msg = fileName + " don't exist or is not a file.";
            logger.error(msg);
            return false;
        }

        Map<String, String> properties = RssUtils.getPropertiesFromFile(fileName);
        properties.forEach((k, v) -> {
            if (SERVICE_PORT.key().equalsIgnoreCase(k)) {
                set(SERVICE_PORT, Integer.valueOf(v));
            }

            if (DATA_STORAGE_TYPE.key().equalsIgnoreCase(k)) {
                set(DATA_STORAGE_TYPE, v.toUpperCase());
            }

            if (DATA_STORAGE_BASE_PATH.key().equalsIgnoreCase(k)) {
                set(DATA_STORAGE_BASE_PATH, v);
            }

            if (BUFFER_CAPACITY.key().equalsIgnoreCase(k)) {
                set(BUFFER_CAPACITY, Integer.valueOf(v));
            }

            if (BUFFER_SIZE.key().equalsIgnoreCase(k)) {
                set(BUFFER_SIZE, Integer.valueOf(v));
            }
        });

        return true;
    }
}
