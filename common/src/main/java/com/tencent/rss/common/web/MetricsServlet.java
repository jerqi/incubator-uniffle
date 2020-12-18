package com.tencent.rss.common.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MetricsServlet extends HttpServlet {
  private CollectorRegistry registry;

  public MetricsServlet() {
    this(CollectorRegistry.defaultRegistry);
  }

  public MetricsServlet(CollectorRegistry registry) {
    this.registry = registry;
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    this.doGet(req, resp);
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.setStatus(200);
    resp.setContentType("text/plain; version=0.0.4; charset=utf-8");
    BufferedWriter writer = new BufferedWriter(resp.getWriter());

    try {
      toJson(writer, getSamples(req));
      writer.flush();
    } finally {
      writer.close();
    }
  }

  private Set<String> parse(HttpServletRequest req) {
    String[] includedParam = req.getParameterValues("name[]");
    return (Set) (includedParam == null ? Collections.emptySet() : new HashSet(Arrays.asList(includedParam)));
  }

  private Enumeration<Collector.MetricFamilySamples> getSamples(HttpServletRequest req) {
    return this.registry.filteredMetricFamilySamples(this.parse(req));
  }

  public void toJson(Writer writer, Enumeration<Collector.MetricFamilySamples> mfs) throws IOException {

    List<Collector.MetricFamilySamples.Sample> metrics = new LinkedList<>();
    while (mfs.hasMoreElements()) {
      Collector.MetricFamilySamples metricFamilySamples = (Collector.MetricFamilySamples) mfs.nextElement();
      metrics.addAll(metricFamilySamples.samples);
    }

    MetricsJsonObj res = new MetricsJsonObj(metrics, System.currentTimeMillis());
    ObjectMapper objectMapper = new ObjectMapper();
    String json = objectMapper.writeValueAsString(res);
    writer.write(json);
  }

  private static class MetricsJsonObj {
    private final List<Collector.MetricFamilySamples.Sample> metrics;
    private final long timeStamp;

    MetricsJsonObj(List<Collector.MetricFamilySamples.Sample> metrics, long timeStamp) {
      this.metrics = metrics;
      this.timeStamp = timeStamp;
    }

    public List<Collector.MetricFamilySamples.Sample> getMetrics() {
      return this.metrics;
    }

    public long getTimeStamp() {
      return this.timeStamp;
    }

  }
}
