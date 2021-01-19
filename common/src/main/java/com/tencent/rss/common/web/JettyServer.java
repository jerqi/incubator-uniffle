package com.tencent.rss.common.web;

import com.tencent.rss.common.config.RssBaseConf;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.servlet.Servlet;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JettyServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(JettyServer.class);

  private Server server;
  private ServletContextHandler servletContextHandler;
  private int httpPort;

  public JettyServer(RssBaseConf conf) throws FileNotFoundException {
    createServer(conf);
  }

  public void createServer(RssBaseConf conf) throws FileNotFoundException {
    httpPort = conf.getInteger(RssBaseConf.JETTY_HTTP_PORT);
    ExecutorThreadPool threadPool = createThreadPool(conf);
    server = new Server(threadPool);
    server.setStopAtShutdown(true);
    server.setStopTimeout(conf.getLong(RssBaseConf.JETTY_STOP_TIMEOUT));
    server.addBean(new ScheduledExecutorScheduler("jetty-thread-pool", true));

    HttpConfiguration httpConfig = new HttpConfiguration();
    addHttpConnector(
        conf.getInteger(RssBaseConf.JETTY_HTTP_PORT),
        httpConfig,
        conf.getLong(RssBaseConf.JETTY_HTTP_IDLE_TIMEOUT));

    setRootServletHandler();

    if (conf.getBoolean(RssBaseConf.JETTY_SSL_ENABLE)) {
      addHttpsConnector(httpConfig, conf);
    }
  }

  public void addServlet(Servlet servlet, String pathSpec) {
    servletContextHandler.addServlet(new ServletHolder(servlet), pathSpec);
    server.setHandler(servletContextHandler);
  }

  private ExecutorThreadPool createThreadPool(RssBaseConf conf) {
    int corePoolSize = conf.getInteger(RssBaseConf.JETTY_CORE_POOL_SIZE);
    int maxPoolSize = conf.getInteger(RssBaseConf.JETTY_MAX_POOL_SIZE);
    ExecutorThreadPool pool = new ExecutorThreadPool(
        corePoolSize, maxPoolSize, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    return pool;
  }

  private void addHttpConnector(int port, HttpConfiguration httpConfig, long idleTimeout) {
    ServerConnector httpConnector = new ServerConnector(server,
        new HttpConnectionFactory(httpConfig));
    httpConnector.setPort(port);
    httpConnector.setIdleTimeout(idleTimeout);
    server.addConnector(httpConnector);
  }

  private void addHttpsConnector(
      HttpConfiguration httpConfig, RssBaseConf conf) throws FileNotFoundException {
    LOGGER.info("Create https connector");
    Path keystorePath = Paths.get(conf.get(RssBaseConf.JETTY_SSL_KEYSTORE_PATH)).toAbsolutePath();
    if (!Files.exists(keystorePath)) {
      throw new FileNotFoundException(keystorePath.toString());
    }

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(keystorePath.toString());
    sslContextFactory.setKeyStorePassword(conf.get(RssBaseConf.JETTY_SSL_KEYSTORE_PASSWORD));
    sslContextFactory.setKeyManagerPassword(conf.get(RssBaseConf.JETTY_SSL_KEYMANAGER_PASSWORD));
    sslContextFactory.setTrustStorePath(keystorePath.toString());
    sslContextFactory.setTrustStorePassword(conf.get(RssBaseConf.JETTY_SSL_TRUSTSTORE_PASSWORD));

    int securePort = conf.getInteger(RssBaseConf.JETTY_HTTPS_PORT);
    httpConfig.setSecurePort(securePort);
    HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
    httpsConfig.addCustomizer(new SecureRequestCustomizer());

    ServerConnector sslConnector = new ServerConnector(server,
        new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
        new HttpConnectionFactory(httpsConfig));
    sslConnector.setPort(securePort);

    server.addConnector(sslConnector);
  }

  private void setRootServletHandler() {
    servletContextHandler = new ServletContextHandler();
    servletContextHandler.setContextPath("/");
    server.setHandler(servletContextHandler);
  }

  public Server getServer() {
    return this.server;
  }

  public void start() throws Exception {
    server.start();
    LOGGER.info("Jetty http server started, listening on port {}", httpPort);
  }

  public void stop() throws Exception {
    server.stop();
  }

  public ServletContextHandler getServletContextHandler() {
    return this.servletContextHandler;
  }
}
