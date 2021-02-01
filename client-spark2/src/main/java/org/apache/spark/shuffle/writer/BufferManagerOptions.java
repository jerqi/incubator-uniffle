package org.apache.spark.shuffle.writer;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferManagerOptions {

  private static final Logger LOG = LoggerFactory.getLogger(BufferManagerOptions.class);

  private long bufferSize;
  private long serializerBufferSize;
  private long serializerBufferMax;
  private long bufferSpillThreshold;

  public BufferManagerOptions(SparkConf sparkConf) {
    bufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_BUFFER_SIZE,
        RssClientConfig.RSS_WRITER_BUFFER_SIZE_DEFAULT_VALUE);
    serializerBufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE,
        RssClientConfig.RSS_WRITER_SERIALIZER_BUFFER_SIZE_DEFAULT_VALUE);
    serializerBufferMax = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_SERIALIZER_BUFFER_MAX_SIZE,
        RssClientConfig.RSS_WRITER_SERIALIZER_BUFFER_MAX_SIZE_DEFAULT_VALUE);
    bufferSpillThreshold = sparkConf.getSizeAsBytes(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE,
        RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE_DEFAULT_VALUE);
    LOG.info(RssClientConfig.RSS_WRITER_BUFFER_SIZE + "=" + bufferSize);
    LOG.info(RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE + "=" + bufferSpillThreshold);
    checkBufferSize();
  }

  private void checkBufferSize() {
    if (bufferSize < 0) {
      throw new RuntimeException("Unexpected value of " + RssClientConfig.RSS_WRITER_BUFFER_SIZE
          + "=" + bufferSize);
    }
    if (bufferSpillThreshold < 0) {
      throw new RuntimeException("Unexpected value of " + RssClientConfig.RSS_WRITER_BUFFER_SPILL_SIZE
          + "=" + bufferSpillThreshold);
    }
  }

  // limit of buffer size is 2G
  public int getBufferSize() {
    return parseToInt(bufferSize);
  }

  public int getSerializerBufferSize() {
    return parseToInt(serializerBufferSize);
  }

  public int getSerializerBufferMax() {
    return parseToInt(serializerBufferMax);
  }

  private int parseToInt(long value) {
    if (value > Integer.MAX_VALUE) {
      value = Integer.MAX_VALUE;
    }
    return (int) value;
  }

  public long getBufferSpillThreshold() {
    return bufferSpillThreshold;
  }
}
