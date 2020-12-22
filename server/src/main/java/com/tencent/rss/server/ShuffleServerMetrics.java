package com.tencent.rss.server;

import com.tencent.rss.common.metrics.MetricsManager;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;

public class ShuffleServerMetrics {
  public static final String TOTAL_REQUEST = "total_request";
  public static final String REGISTER_REQUEST = "register_request";
  public static final String SEND_DATA_REQUEST = "send_data_request";
  public static final String COMMIT_REQUEST = "commit_request";
  public static final String REGISTERED_SHUFFLE = "registered_shuffle";
  public static final String REGISTERED_SHUFFLE_ENGINE = "registered_shuffle_engine";
  public static final String AVAILABLE_BUFFER = "available_buffer";
  public static final String BUFFERED_BLOCK_SIZE = "buffered_block_size";
  public static final String BUFFERED_BLOCK_NUM = "buffered_block_num";
  public static final String BLOCK_WRITE_SIZE = "block_write_size";
  public static final String BLOCK_WRITE_NUM = "block_write_num";
  public static final String INDEX_WRITE_SIZE = "index_write_size";
  public static final String INDEX_WRITE_NUM = "index_write_num";

  private static MetricsManager metricsManager;

  private static Gauge totalRequest;
  private static Gauge registerRequest;
  private static Gauge sendDataRequest;
  private static Gauge commitRequest;

  private static Gauge registeredShuffle;
  private static Gauge registeredShuffleEngine;

  private static Gauge availableBuffer;
  private static Gauge bufferedBlockSize;
  private static Gauge bufferedBlockNum;

  private static Gauge blockWriteNum;
  private static Gauge blockWriteSize;
  private static Gauge indexWriteSize;
  private static Gauge indexWriteNum;

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
    totalRequest = metricsManager.addGauge(TOTAL_REQUEST);
    registerRequest = metricsManager.addGauge(REGISTER_REQUEST);
    sendDataRequest = metricsManager.addGauge(SEND_DATA_REQUEST);
    commitRequest = metricsManager.addGauge(COMMIT_REQUEST);

    registeredShuffle = metricsManager.addGauge(REGISTERED_SHUFFLE);
    registeredShuffleEngine = metricsManager.addGauge(REGISTERED_SHUFFLE_ENGINE);

    availableBuffer = metricsManager.addGauge(AVAILABLE_BUFFER);
    bufferedBlockSize = metricsManager.addGauge(BUFFERED_BLOCK_SIZE);
    bufferedBlockNum = metricsManager.addGauge(BUFFERED_BLOCK_NUM);

    blockWriteSize = metricsManager.addGauge(BLOCK_WRITE_SIZE);
    blockWriteNum = metricsManager.addGauge(BLOCK_WRITE_NUM);
    indexWriteSize = metricsManager.addGauge(INDEX_WRITE_SIZE);
    indexWriteNum = metricsManager.addGauge(INDEX_WRITE_NUM);

  }

  public static void incTotalRequest() {
    totalRequest.inc();
  }

  public static void decTotalRequest() {
    totalRequest.dec();
  }

  public static void incRegisterRequest() {
    registerRequest.inc();
  }

  public static void decRegisterRequest() {
    registerRequest.dec();
  }

  public static void incSendDataRequest() {
    sendDataRequest.inc();
  }

  public static void decSendDataRequest() {
    sendDataRequest.dec();
  }

  public static void incCommitRequest() {
    commitRequest.inc();
  }

  public static void decCommitRequest() {
    commitRequest.dec();
  }

  public static void incRegisteredShuffle() {
    registeredShuffle.inc();
  }

  public static void decRegisteredShuffle() {
    registeredShuffle.dec();
  }

  public static void incRegisteredShuffleEngine() {
    registeredShuffleEngine.inc();
  }

  public static void decRegisteredShuffleEngine() {
    registeredShuffleEngine.dec();
  }

  public static void incAvailableBuffer(int d) {
    availableBuffer.inc((double) d);
  }

  public static void decAvailableBuffer(int d) {
    availableBuffer.dec((double) d);
  }

  public static void incBufferedBlockSize(int d) {
    bufferedBlockSize.inc((double) d);
  }

  public static void decBufferedBlockSize(int d) {
    bufferedBlockSize.dec((double) d);
  }

  public static void setBufferedBlockSize(int d) {
    bufferedBlockSize.set((double) d);
  }

  public static void incBufferedBlockNum(int d) {
    bufferedBlockNum.inc((double) d);
  }

  public static void decBufferedBlockNum(int d) {
    bufferedBlockNum.dec((double) d);
  }

  public static void setBufferedBlockNum(int d) {
    bufferedBlockNum.set((double) d);
  }

  public static void incBlockWriteSize(int d) {
    blockWriteSize.inc((double) d);
  }

  public static void decBlockWriteSize(int d) {
    blockWriteSize.dec((double) d);
  }

  public static void incBlockWriteNum(int d) {
    blockWriteNum.inc((double) d);
  }

  public static void decBlockWriteNum(int d) {
    blockWriteNum.dec((double) d);
  }

  public static void incIndexWriteSize(int d) {
    indexWriteSize.inc((double) d);
  }

  public static void decIndexWriteSize(int d) {
    indexWriteSize.dec((double) d);
  }

  public static void incIndexWriteNum(int d) {
    indexWriteNum.inc((double) d);
  }

  public static void decIndexWriteNum(int d) {
    indexWriteNum.dec((double) d);
  }

}
