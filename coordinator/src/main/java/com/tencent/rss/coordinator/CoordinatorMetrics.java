package com.tencent.rss.coordinator;

import com.tencent.rss.common.metrics.MetricsManager;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;

public class CoordinatorMetrics {

  private static final String TOTAL_SERVER_NUM = "total_server_num";

  static Gauge gaugeTotalServerNum;


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
  }
}
