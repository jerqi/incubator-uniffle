package org.apache.spark.shuffle.writer;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferManagerOptions {

    private static final Logger LOG = LoggerFactory.getLogger(BufferManagerOptions.class);

    private int individualBufferSize;
    private int individualBufferMax;
    private int bufferSpillThreshold;

    public BufferManagerOptions() {
    }

    public BufferManagerOptions(SparkConf sparkConf) {
        individualBufferSize = sparkConf.getInt(RssClientConfig.RSS_WRITER_BUFFER_SIZE,
                RssClientConfig.RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE);
        individualBufferMax = sparkConf.getInt(RssClientConfig.RSS_WRITER_BUFFER_MAX_SIZE,
                RssClientConfig.RSS_WRITER_BUFFER_MAX_SIZE_DEFAULT_VALUE);
        bufferSpillThreshold = sparkConf.getInt(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE,
                RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE_DEFAULT_VALUE);
        LOG.info(RssClientConfig.RSS_WRITER_BUFFER_SIZE + "=" + individualBufferSize);
        LOG.info(RssClientConfig.RSS_WRITER_BUFFER_MAX_SIZE + "=" + individualBufferMax);
        LOG.info(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE + "=" + bufferSpillThreshold);
        checkBufferSize();
    }

    private void checkBufferSize() {
        if (individualBufferSize < 0) {
            throw new RuntimeException("Unexpected value of " + RssClientConfig.RSS_WRITER_BUFFER_SIZE
                    + "=" + individualBufferSize);
        }
        if (individualBufferMax < individualBufferSize) {
            throw new RuntimeException(RssClientConfig.RSS_WRITER_BUFFER_MAX_SIZE + " should be great than "
                    + RssClientConfig.RSS_WRITER_BUFFER_SIZE + ", " + RssClientConfig.RSS_WRITER_BUFFER_MAX_SIZE
                    + "=" + individualBufferMax + ", " + RssClientConfig.RSS_WRITER_BUFFER_SIZE + "="
                    + individualBufferSize);
        }
        if (bufferSpillThreshold < 0) {
            throw new RuntimeException("Unexpected value of " + RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE
                    + "=" + bufferSpillThreshold);
        }
    }

    public int getIndividualBufferSize() {
        return individualBufferSize;
    }

    public int getIndividualBufferMax() {
        return individualBufferMax;
    }

    public int getBufferSpillThreshold() {
        return bufferSpillThreshold;
    }
}
