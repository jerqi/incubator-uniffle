package com.tencent.rss.server;

import com.tencent.rss.common.metrics.MetricsManager;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public class ShuffleServerMetrics {

  private static final String TOTAL_REQUEST = "total_request";
  private static final String REGISTER_REQUEST = "register_request";
  private static final String SEND_DATA_REQUEST = "send_data_request";
  private static final String COMMIT_REQUEST = "commit_request";

  private static final String TOTAL_RECEIVED_DATA = "total_received_data";
  private static final String TOTAL_WRITE_DATA = "total_write_data";
  private static final String TOTAL_WRITE_BLOCK = "total_write_block";
  private static final String TOTAL_WRITE_TIME = "total_write_time";

  private static final String REGISTERED_SHUFFLE = "registered_shuffle";
  private static final String REGISTERED_SHUFFLE_ENGINE = "registered_shuffle_engine";
  private static final String BUFFERED_DATA_SIZE = "buffered_data_size";
  private static final String ALLOCATED_BUFFER_SIZE = "allocated_buffer_size";
  private static final String IN_FLUSH_BUFFER_SIZE = "in_flush_buffer_size";
  private static final String USED_BUFFER_SIZE = "used_buffer_size";

  private static final String WRITE_SPEED = "write_speed";

  static Counter counterTotalRequest;
  static Counter counterRegisterRequest;
  static Counter counterSendDataRequest;
  static Counter counterCommitRequest;
  static Counter counterTotalReceivedDataSize;
  static Counter counterTotalWriteDataSize;
  static Counter counterTotalWriteBlockSize;
  static Counter counterTotalWriteTime;

  static Gauge gaugeRegisteredShuffle;
  static Gauge gaugeRegisteredShuffleEngine;
  static Gauge gaugeBufferDataSize;
  static Gauge gaugeAllocatedBufferSize;
  static Gauge gaugeInFlushBufferSize;
  static Gauge gaugeUsedBufferSize;

  static Histogram histogramWriteSpeed;

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  public static void register() {
    register(CollectorRegistry.defaultRegistry);
  }

  public static synchronized void register(CollectorRegistry collectorRegistry) {
    if (!isRegister) {
      metricsManager = new MetricsManager(collectorRegistry);
      isRegister = true;
      setUpMetrics();
    }
  }

  public static CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  private static void setUpMetrics() {
    counterTotalRequest = metricsManager.addCounter(TOTAL_REQUEST);
    counterRegisterRequest = metricsManager.addCounter(REGISTER_REQUEST);
    counterSendDataRequest = metricsManager.addCounter(SEND_DATA_REQUEST);
    counterCommitRequest = metricsManager.addCounter(COMMIT_REQUEST);
    counterTotalReceivedDataSize = metricsManager.addCounter(TOTAL_RECEIVED_DATA);
    counterTotalWriteDataSize = metricsManager.addCounter(TOTAL_WRITE_DATA);
    counterTotalWriteBlockSize = metricsManager.addCounter(TOTAL_WRITE_BLOCK);
    counterTotalWriteTime = metricsManager.addCounter(TOTAL_WRITE_TIME);

    gaugeRegisteredShuffle = metricsManager.addGauge(REGISTERED_SHUFFLE);
    gaugeRegisteredShuffleEngine = metricsManager.addGauge(REGISTERED_SHUFFLE_ENGINE);
    gaugeBufferDataSize = metricsManager.addGauge(BUFFERED_DATA_SIZE);
    gaugeAllocatedBufferSize = metricsManager.addGauge(ALLOCATED_BUFFER_SIZE);
    gaugeInFlushBufferSize = metricsManager.addGauge(IN_FLUSH_BUFFER_SIZE);
    gaugeUsedBufferSize = metricsManager.addGauge(USED_BUFFER_SIZE);

    final double mb = 1024 * 1024;
    final double[] buckets = {1.0 * mb, 10.0 * mb, 20.0 * mb, 30.0 * mb, 50.0 * mb, 80 * mb, 100.0 * mb};
    histogramWriteSpeed = metricsManager.addHistogram(WRITE_SPEED, buckets);
  }

}
