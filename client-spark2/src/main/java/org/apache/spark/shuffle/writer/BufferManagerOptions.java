package org.apache.spark.shuffle.writer;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferManagerOptions {

  private static final Logger LOG = LoggerFactory.getLogger(BufferManagerOptions.class);

  private long individualBufferSize;
  private long individualBufferMax;
  private long bufferSpillThreshold;

  public BufferManagerOptions(SparkConf sparkConf) {
    individualBufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_BUFFER_SIZE,
        RssClientConfig.RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE);
    individualBufferMax = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_BUFFER_MAX_SIZE,
        RssClientConfig.RSS_WRITER_BUFFER_MAX_SIZE_DEFAULT_VALUE);
    bufferSpillThreshold = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE,
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

  // limit of buffer size is 2G
  public int getIndividualBufferSize() {
    if (individualBufferSize > Integer.MAX_VALUE) {
      individualBufferSize = Integer.MAX_VALUE;
    }
    return (int) individualBufferSize;
  }

  public int getIndividualBufferMax() {
    if (individualBufferMax > Integer.MAX_VALUE) {
      individualBufferMax = Integer.MAX_VALUE;
    }
    return (int) individualBufferMax;
  }

  public long getBufferSpillThreshold() {
    return bufferSpillThreshold;
  }
}
