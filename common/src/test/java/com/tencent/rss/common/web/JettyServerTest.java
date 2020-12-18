package com.tencent.rss.common.web;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class JettyServerTest {
  private static final String CONFIG_FILE = ClassLoader.getSystemResource("server.conf").getFile();

  @Test
  public void jettyServerTest() throws FileNotFoundException {
    JettyConf conf = new JettyConf();
    conf.loadConfFromFile(CONFIG_FILE);
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

}
