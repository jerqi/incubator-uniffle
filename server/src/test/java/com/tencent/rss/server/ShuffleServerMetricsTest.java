package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.rss.common.metrics.JvmMetrics;
import com.tencent.rss.common.web.CommonMetricsServlet;
import com.tencent.rss.common.web.JettyServer;
import io.prometheus.client.CollectorRegistry;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShuffleServerMetricsTest {

  private static final String SERVER_METRICS_URL = "http://127.0.0.1:12345/metrics/server";
  private static final String SERVER_JVM_URL = "http://127.0.0.1:12345/metrics/jvm";
  private static JettyServer server;

  @BeforeClass
  public static void setUp() throws Exception {
    ShuffleServerConf ssc = new ShuffleServerConf();
    ssc.setString("rss.jetty.http.port", "12345");
    ssc.setString("rss.jetty.corePool.size", "128");
    server = new JettyServer(ssc);
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    ShuffleServerMetrics.register(shuffleServerCollectorRegistry);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry);
    server.addServlet(new CommonMetricsServlet(ShuffleServerMetrics.getCollectorRegistry()), "/metrics/server");
    server.addServlet(new CommonMetricsServlet(JvmMetrics.getCollectorRegistry()), "/metrics/jvm");
    server.start();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    server.stop();
  }

  private String httpGetMetrics(String urlString) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    BufferedReader in = new BufferedReader(
        new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer content = new StringBuffer();
    while ((inputLine = in.readLine()) != null) {
      content.append(inputLine);
    }
    in.close();
    return content.toString();
  }

  @Test
  public void testJvmMetrics() throws Exception {
    String content = httpGetMetrics(SERVER_JVM_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
  }

  @Test
  public void testServerMetrics() throws Exception {
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterTotalReceivedDataSize.inc();

    String content = httpGetMetrics(SERVER_METRICS_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
    assertEquals(29, actualObj.get("metrics").size());
  }

  @Test
  public void testServerMetricsConcurrently() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    List<Callable<Void>> calls = new ArrayList<>();
    ShuffleServerMetrics.gaugeBufferDataSize.set(0);

    long expectedNum = 0;
    for (int i = 1; i < 5; ++i) {
      int cur = i * i;
      if (i % 2 == 0) {
        calls.add(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ShuffleServerMetrics.gaugeBufferDataSize.inc(cur);
            return null;
          }
        });
        expectedNum += cur;
      } else {
        calls.add(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ShuffleServerMetrics.gaugeBufferDataSize.dec(cur);
            return null;
          }
        });
        expectedNum -= cur;
      }
    }

    List<Future<Void>> results = executorService.invokeAll(calls);
    for (Future f : results) {
      f.get();
    }

    String content = httpGetMetrics(SERVER_METRICS_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);

    final long tmp = expectedNum;
    actualObj.get("metrics").iterator().forEachRemaining(jsonNode -> {
      String name = jsonNode.get("name").textValue();
      if (name.equals("buffered_data_size")) {
        assertEquals(tmp, jsonNode.get("value").asLong());
      }
    });
  }
}
