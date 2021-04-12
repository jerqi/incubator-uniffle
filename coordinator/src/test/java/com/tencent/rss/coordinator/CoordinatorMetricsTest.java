package com.tencent.rss.coordinator;

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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoordinatorMetricsTest {

  private static final String SERVER_METRICS_URL = "http://127.0.0.1:12345/metrics/server";
  private static final String SERVER_JVM_URL = "http://127.0.0.1:12345/metrics/jvm";
  private static JettyServer server;

  @BeforeClass
  public static void setUp() throws Exception {
    CoordinatorConf ssc = new CoordinatorConf();
    ssc.setString("rss.jetty.http.port", "12345");
    ssc.setString("rss.jetty.corePool.size", "128");
    server = new JettyServer(ssc);
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    CoordinatorMetrics.register(shuffleServerCollectorRegistry);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry);
    server.addServlet(new CommonMetricsServlet(CoordinatorMetrics.getCollectorRegistry()), "/metrics/server");
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
  public void testCoordinatorMetrics() throws Exception {
    CoordinatorMetrics.gaugeTotalServerNum.inc();
    CoordinatorMetrics.gaugeTotalServerNum.inc();

    String content = httpGetMetrics(SERVER_METRICS_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
  }

  @Test
  public void testJvmMetrics() throws Exception {
    String content = httpGetMetrics(SERVER_JVM_URL);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode actualObj = mapper.readTree(content);
    assertEquals(2, actualObj.size());
  }

}
