package com.tencent.rss.test.utils;

import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class LoggedIterator implements Iterator<Tuple2<String, String>> {

  private static final Logger LOG = LoggerFactory.getLogger(LoggedIterator.class);

  private long startTime = 0L;
  private long totalBytes = 0L;
  private int partitionId;
  private Iterator<Tuple2<String, String>> underlyingIterator;

  public LoggedIterator(Iterator<Tuple2<String, String>> underlyingIterator, int partitionId) {
    this.underlyingIterator = underlyingIterator;
    this.partitionId = partitionId;
  }

  public boolean hasNext() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    boolean result = underlyingIterator.hasNext();
    if (!result) {
      long duration = System.currentTimeMillis() - startTime;
      String throughput = "";
      if (duration == 0) {
        throughput = "(unknown)";
      } else {
        throughput = totalBytes * 1.0 / (1024 * 1024) / (duration * 1.0 / 1000.0) + " mb/s";
      }
      LOG.info("Partition " + partitionId + ", iterator throughput: " + throughput);
    }

    return result;
  }

  public Tuple2<String, String> next() {
    Tuple2<String, String> result = underlyingIterator.next();
    if (result != null) {
      if (result._1 != null) {
        totalBytes = totalBytes + result._1.length();
      }
      if (result._2 != null) {
        totalBytes = totalBytes + result._2.length();
      }
    }
    return result;
  }
}
