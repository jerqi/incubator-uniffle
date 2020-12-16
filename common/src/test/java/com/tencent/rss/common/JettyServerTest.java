package com.tencent.rss.common;

import com.tencent.rss.common.web.JettyConf;
import com.tencent.rss.common.web.JettyServer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class JettyServerTest {
  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();

  @Test
  public void jettyServerTest() throws FileNotFoundException {
    JettyConf conf = new JettyConf();
    conf.loadConfFromFile(confFile);
    JettyServer jettyServer = new JettyServer();
    jettyServer.createServer(conf);
    Server server = jettyServer.getServer();

    assertEquals(4, server.getBeans().size());
    assertEquals(30000, server.getStopTimeout());

    assertTrue(server.getThreadPool() instanceof QueuedThreadPool);
    QueuedThreadPool threadPool = (QueuedThreadPool) server.getThreadPool();
    assertEquals(8, threadPool.getMinThreads());
    assertEquals(32, threadPool.getMaxThreads());


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
