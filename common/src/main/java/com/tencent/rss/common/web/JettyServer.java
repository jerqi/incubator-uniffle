package com.tencent.rss.common.web;

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

import javax.servlet.Servlet;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class JettyServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(JettyServer.class);

  private Server server;
  private ServletContextHandler servletContextHandler;

  public JettyServer(String confFileName) throws Exception {
    JettyConf jettyConf = new JettyConf();
    jettyConf.loadConfFromFile(confFileName);
    createServer(jettyConf);
  }

  public JettyServer(JettyConf conf) throws FileNotFoundException {
    createServer(conf);
  }

  public void createServer(JettyConf conf) throws FileNotFoundException {
    ExecutorThreadPool threadPool = createThreadPool(conf);
    server = new Server(threadPool);
    server.setStopAtShutdown(true);
    server.setStopTimeout(conf.getLong(JettyConf.JETTY_STOP_TIMEOUT));
    server.addBean(new ScheduledExecutorScheduler("jetty-thread-pool", true));

    HttpConfiguration httpConfig = new HttpConfiguration();
    addHttpConnector(
      conf.getInteger(JettyConf.JETTY_HTTP_PORT),
      httpConfig,
      conf.getLong(JettyConf.JETTY_HTTP_IDLE_TIMEOUT));

    setRootServletHandler();

    if (conf.getBoolean(JettyConf.JETTY_SSL_ENABLE)) {
      addHttpsConnector(httpConfig, conf);
    }
  }

  public void addServlet(Servlet servlet, String pathSpec) {
    servletContextHandler.addServlet(new ServletHolder(servlet), pathSpec);
    server.setHandler(servletContextHandler);
  }

  private ExecutorThreadPool createThreadPool(JettyConf conf) {
    int corePoolSize = conf.getInteger(JettyConf.JETTY_CORE_POOL_SIZE);
    int queueSize = conf.getInteger(JettyConf.JETTY_QUEUE_SIZE);
    ExecutorThreadPool pool = new ExecutorThreadPool(
      corePoolSize, corePoolSize, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize));
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
    HttpConfiguration httpConfig, JettyConf conf) throws FileNotFoundException {
    LOGGER.info("Create https connector");
    Path keystorePath = Paths.get(conf.get(JettyConf.JETTY_SSL_KEYSTORE_PATH)).toAbsolutePath();
    if (!Files.exists(keystorePath)) {
      throw new FileNotFoundException(keystorePath.toString());
    }

    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setKeyStorePath(keystorePath.toString());
    sslContextFactory.setKeyStorePassword(conf.get(JettyConf.JETTY_SSL_KEYSTORE_PASSWORD));
    sslContextFactory.setKeyManagerPassword(conf.get(JettyConf.JETTY_SSL_KEYMANAGER_PASSWORD));
    sslContextFactory.setTrustStorePath(keystorePath.toString());
    sslContextFactory.setTrustStorePassword(conf.get(JettyConf.JETTY_SSL_TRUSTSTORE_PASSWORD));

    int securePort = conf.getInteger(JettyConf.JETTY_HTTPS_PORT);
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
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          server.stop();
        } catch (Exception e) {
          LOGGER.error(e.getMessage());
        }
      }
    });
  }

  public void stop() throws Exception {
    this.server.stop();
  }

  public ServletContextHandler getServletContextHandler() {
    return this.servletContextHandler;
  }

}
