package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.rss.common.metrics.JvmMetrics;
import com.tencent.rss.common.web.JettyServer;
import com.tencent.rss.common.web.MetricsServlet;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ShuffleServerMetricsTest {

  private static final String SERVER_METRICS_URL = "http://localhost:12345/metrics/server";
  private static final String SERVER_JVM_URL = "http://localhost:12345/metrics/jvm";
  private static JettyServer server;

  @BeforeClass
  public static void setUp() throws Exception {
    ShuffleServerConf ssc = new ShuffleServerConf();
    ssc.setString("jetty.http.port", "12345");
    ssc.setString("jetty.corePool.size", "64");
    server = new JettyServer(ssc);
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    ShuffleServerMetrics.register(shuffleServerCollectorRegistry);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry);
    server.addServlet(new MetricsServlet(ShuffleServerMetrics.getCollectorRegistry()), "/metrics/server");
    server.addServlet(new MetricsServlet(JvmMetrics.getCollectorRegistry()), "/metrics/jvm");
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
    ShuffleServerMetrics.incTotalRequest();
    ShuffleServerMetrics.incTotalRequest();
    ShuffleServerMetrics.decTotalRequest();
    ShuffleServerMetrics.incBlockWriteNum(1024);

    String content = httpGetMetrics(SERVER_METRICS_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
    assertEquals(13, actualObj.get("metrics").size());
  }

  @Test
  public void testServerMetricsConcurrently() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    List<Callable<Void>> calls = new ArrayList<>();

    long expectedNum = 0;
    for (int i = 1; i < 5; ++i) {
      int cur = i * i;
      if (i % 2 == 0) {
        calls.add(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ShuffleServerMetrics.incIndexWriteSize(cur);
            return null;
          }
        });
        expectedNum += cur;
      } else {
        calls.add(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            ShuffleServerMetrics.decIndexWriteSize(cur);
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
      if (name.equals(ShuffleServerMetrics.INDEX_WRITE_SIZE)) {
        assertEquals(tmp, jsonNode.get("value").asLong());
      }
    });
  }
}
