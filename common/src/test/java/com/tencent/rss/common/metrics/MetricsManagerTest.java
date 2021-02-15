package com.tencent.rss.common.metrics;

import static io.prometheus.client.Collector.MetricFamilySamples;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class MetricsManagerTest {

  @Test
  public void testMetricsManager() {
    MetricsManager metricsManager = new MetricsManager();
    assertEquals(CollectorRegistry.defaultRegistry, metricsManager.getCollectorRegistry());

    CollectorRegistry expectedRegistry = new CollectorRegistry();
    metricsManager = new MetricsManager(expectedRegistry);
    assertEquals(expectedRegistry, metricsManager.getCollectorRegistry());

    String expectedName1 = "counter";
    String expectedHelp1 = "Counter " + expectedName1;
    metricsManager.addCounter(expectedName1);

    String expectedName2 = "name2";
    String expectedHelp2 = "Gauge " + expectedName2;
    String label = "gaugeLabel";
    Gauge gauge = metricsManager.addGauge(expectedName2, label);
    gauge.labels("lv1").inc();
    gauge.labels("lv2").inc();

    Map<String, MetricFamilySamples> metricsSamples = new HashMap<>();
    Enumeration<MetricFamilySamples> mfs = expectedRegistry.metricFamilySamples();
    while (mfs.hasMoreElements()) {
      MetricFamilySamples cur = mfs.nextElement();
      metricsSamples.put(cur.name, cur);
    }

    assertEquals(expectedHelp1, metricsSamples.get(expectedName1).help);
    assertEquals(1, metricsSamples.get(expectedName1).samples.size());

    assertEquals(expectedHelp2, metricsSamples.get(expectedName2).help);
    List<MetricFamilySamples.Sample> f = metricsSamples.get(expectedName2).samples;
    assertEquals(2, metricsSamples.get(expectedName2).samples.size());
    String[] actualLabelValues = metricsSamples
        .get(expectedName2).samples
        .stream().map(i -> i.labelValues.get(0))
        .collect(Collectors.toList()).toArray(new String[0]);
    Arrays.sort(actualLabelValues);
    assertArrayEquals(new String[]{"lv1", "lv2"}, actualLabelValues);
  }
}
