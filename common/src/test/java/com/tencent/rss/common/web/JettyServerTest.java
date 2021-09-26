package com.tencent.rss.common.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.ExitUtils;
import com.tencent.rss.common.util.ExitUtils.ExitException;
import java.io.FileNotFoundException;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.junit.Test;

public class JettyServerTest {

  @Test
  public void jettyServerTest() throws FileNotFoundException {
    RssBaseConf conf = new RssBaseConf();
    conf.setString("rss.jetty.http.port", "9527");
    JettyServer jettyServer = new JettyServer(conf);
    Server server = jettyServer.getServer();

    assertEquals(4, server.getBeans().size());
    assertEquals(30000, server.getStopTimeout());
    assertTrue(server.getThreadPool() instanceof ExecutorThreadPool);

    assertEquals(1, server.getConnectors().length);
    assertEquals(server, server.getHandler().getServer());
    assertTrue(server.getConnectors()[0] instanceof ServerConnector);
    ServerConnector connector = (ServerConnector) server.getConnectors()[0];
    assertEquals(9527, connector.getPort());

    assertEquals(1, server.getHandlers().length);
    Handler handler = server.getHandler();
    assertTrue(handler instanceof ServletContextHandler);
  }

  @Test
  public void jettyServerStartTest() throws Exception {
    try {
      RssBaseConf conf = new RssBaseConf();
      conf.setString("rss.jetty.http.port", "9527");
      JettyServer jettyServer1 = new JettyServer(conf);
      JettyServer jettyServer2 = new JettyServer(conf);
      jettyServer1.start();

      ExitUtils.disableSystemExit();
      final String expectMessage = "Fail to start jetty http server";
      final int expectStatus = 1;
      try {
        jettyServer2.start();
      } catch (Exception e) {
        assertEquals(expectMessage, e.getMessage());
        assertEquals(expectStatus, ((ExitException) e).getStatus());
      }

      final Thread t = new Thread(null, () -> {
        throw new AssertionError("TestUncaughtException");
      }, "testThread");
      t.start();
      t.join();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }

}
