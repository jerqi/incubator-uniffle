package com.tencent.rss.common.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

public class MetricsManager {
  private CollectorRegistry collectorRegistry;

  public MetricsManager() {
    this(null);
  }

  public MetricsManager(CollectorRegistry collectorRegistry) {
    if (collectorRegistry == null) {
      this.collectorRegistry = CollectorRegistry.defaultRegistry;
    } else {
      this.collectorRegistry = collectorRegistry;
    }
  }

  public CollectorRegistry getCollectorRegistry() {
    return this.collectorRegistry;
  }

  public Counter addCounter(String name, String... labels) {
    return addCounter(name, "Counter " + name, labels);
  }

  public Counter addCounter(String name, String help, String[] labels) {
    return Counter.build()
      .name(name).labelNames(labels).help(help).register(collectorRegistry);
  }

  public Gauge addGauge(String name, String... labels) {
    return addGauge(name, "Gauge " + name, labels);
  }

  public Gauge addGauge(String name, String help, String[] labels) {
    return Gauge.build()
      .name(name).labelNames(labels).help(help).register(collectorRegistry);
  }
}
