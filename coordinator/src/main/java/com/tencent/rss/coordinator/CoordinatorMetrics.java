package com.tencent.rss.coordinator;

import com.tencent.rss.common.metrics.MetricsManager;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public class CoordinatorMetrics {

  private static final String TOTAL_SERVER_NUM = "total_server_num";
  private static final String RUNNING_APP_NUM = "running_app_num";
  private static final String TOTAL_APP_NUM = "total_app_num";
  private static final String EXCLUDE_SERVER_NUM = "exclude_server_num";

  static Gauge gaugeTotalServerNum;
  static Gauge gaugeExcludeServerNum;
  static Gauge gaugeRunningAppNum;
  static Counter counterTotalAppNum;

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  public static synchronized void register(CollectorRegistry collectorRegistry) {
    if (!isRegister) {
      metricsManager = new MetricsManager(collectorRegistry);
      isRegister = true;
      setUpMetrics();
    }
  }

  public static void register() {
    register(CollectorRegistry.defaultRegistry);
  }

  public static CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  private static void setUpMetrics() {
    gaugeTotalServerNum = metricsManager.addGauge(TOTAL_SERVER_NUM);
    gaugeExcludeServerNum = metricsManager.addGauge(EXCLUDE_SERVER_NUM);
    gaugeRunningAppNum = metricsManager.addGauge(RUNNING_APP_NUM);
    counterTotalAppNum = metricsManager.addCounter(TOTAL_APP_NUM);
  }
}
