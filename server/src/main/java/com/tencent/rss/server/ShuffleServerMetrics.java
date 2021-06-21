package com.tencent.rss.server;

import com.tencent.rss.common.metrics.MetricsManager;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public class ShuffleServerMetrics {

  private static final String TOTAL_REQUEST = "total_request";
  private static final String REGISTER_REQUEST = "register_request";
  private static final String SEND_DATA_REQUEST = "send_data_request";
  private static final String COMMIT_REQUEST = "commit_request";

  private static final String TOTAL_RECEIVED_DATA = "total_received_data";
  private static final String TOTAL_WRITE_DATA = "total_write_data";
  private static final String TOTAL_WRITE_BLOCK = "total_write_block";
  private static final String TOTAL_WRITE_TIME = "total_write_time";
  private static final String TOTAL_WRITE_HANDLER = "total_write_handler";
  private static final String TOTAL_WRITE_EXCEPTION = "total_write_exception";
  private static final String TOTAL_WRITE_SLOW = "total_write_slow";
  private static final String TOTAL_WRITE_NUM = "total_write_num";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL1 = "event_size_threshold_level1";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL2 = "event_size_threshold_level2";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL3 = "event_size_threshold_level3";
  private static final String EVENT_SIZE_THRESHOLD_LEVEL4 = "event_size_threshold_level4";
  private static final String EVENT_QUEUE_SIZE = "event_queue_size";
  private static final String TOTAL_READ_DATA = "total_read_data";
  private static final String TOTAL_READ_TIME = "total_read_time";

  private static final String REGISTERED_SHUFFLE = "registered_shuffle";
  private static final String REGISTERED_SHUFFLE_ENGINE = "registered_shuffle_engine";
  private static final String BUFFERED_DATA_SIZE = "buffered_data_size";
  private static final String ALLOCATED_BUFFER_SIZE = "allocated_buffer_size";
  private static final String IN_FLUSH_BUFFER_SIZE = "in_flush_buffer_size";
  private static final String USED_BUFFER_SIZE = "used_buffer_size";

  static Counter counterTotalRequest;
  static Counter counterRegisterRequest;
  static Counter counterSendDataRequest;
  static Counter counterCommitRequest;
  static Counter counterTotalReceivedDataSize;
  static Counter counterTotalWriteDataSize;
  static Counter counterTotalWriteBlockSize;
  static Counter counterTotalWriteTime;
  static Counter counterWriteException;
  static Counter counterWriteSlow;
  static Counter counterWriteTotal;
  static Counter counterEventSizeThresholdLevel1;
  static Counter counterEventSizeThresholdLevel2;
  static Counter counterEventSizeThresholdLevel3;
  static Counter counterEventSizeThresholdLevel4;
  static Counter counterTotalReadDataSize;
  static Counter counterTotalReadTime;

  static Gauge gaugeRegisteredShuffle;
  static Gauge gaugeRegisteredShuffleEngine;
  static Gauge gaugeBufferDataSize;
  static Gauge gaugeAllocatedBufferSize;
  static Gauge gaugeInFlushBufferSize;
  static Gauge gaugeUsedBufferSize;
  static Gauge gaugeWriteHandler;
  static Gauge gaugeEventQueueSize;

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
    counterWriteException = metricsManager.addCounter(TOTAL_WRITE_EXCEPTION);
    counterWriteSlow = metricsManager.addCounter(TOTAL_WRITE_SLOW);
    counterWriteTotal = metricsManager.addCounter(TOTAL_WRITE_NUM);
    counterEventSizeThresholdLevel1 = metricsManager.addCounter(EVENT_SIZE_THRESHOLD_LEVEL1);
    counterEventSizeThresholdLevel2 = metricsManager.addCounter(EVENT_SIZE_THRESHOLD_LEVEL2);
    counterEventSizeThresholdLevel3 = metricsManager.addCounter(EVENT_SIZE_THRESHOLD_LEVEL3);
    counterEventSizeThresholdLevel4 = metricsManager.addCounter(EVENT_SIZE_THRESHOLD_LEVEL4);
    counterTotalReadDataSize = metricsManager.addCounter(TOTAL_READ_DATA);
    counterTotalReadTime = metricsManager.addCounter(TOTAL_READ_TIME);

    gaugeRegisteredShuffle = metricsManager.addGauge(REGISTERED_SHUFFLE);
    gaugeRegisteredShuffleEngine = metricsManager.addGauge(REGISTERED_SHUFFLE_ENGINE);
    gaugeBufferDataSize = metricsManager.addGauge(BUFFERED_DATA_SIZE);
    gaugeAllocatedBufferSize = metricsManager.addGauge(ALLOCATED_BUFFER_SIZE);
    gaugeInFlushBufferSize = metricsManager.addGauge(IN_FLUSH_BUFFER_SIZE);
    gaugeUsedBufferSize = metricsManager.addGauge(USED_BUFFER_SIZE);
    gaugeWriteHandler = metricsManager.addGauge(TOTAL_WRITE_HANDLER);
    gaugeEventQueueSize = metricsManager.addGauge(EVENT_QUEUE_SIZE);
  }

}
