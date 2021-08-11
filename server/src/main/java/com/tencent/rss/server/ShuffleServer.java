package com.tencent.rss.server;

import com.tencent.rss.common.Arguments;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.metrics.JvmMetrics;
import com.tencent.rss.common.rpc.ServerInterface;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.common.web.CommonMetricsServlet;
import com.tencent.rss.common.web.JettyServer;
import com.tencent.rss.storage.util.StorageType;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class ShuffleServer {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServer.class);
  private RegisterHeartBeat registerHeartBeat;
  private String id;
  private String ip;
  private int port;
  private ShuffleServerConf shuffleServerConf;
  private JettyServer jettyServer;
  private ShuffleTaskManager shuffleTaskManager;
  private ServerInterface server;
  private ShuffleFlushManager shuffleFlushManager;
  private ShuffleBufferManager shuffleBufferManager;
  private MultiStorageManager multiStorageManager;

  public ShuffleServer(ShuffleServerConf shuffleServerConf) throws Exception {
    this.shuffleServerConf = shuffleServerConf;
    initialization();
  }

  public ShuffleServer(String configFile) throws Exception {
    this.shuffleServerConf = new ShuffleServerConf(configFile);
    initialization();
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOG.info("Start to init shuffle server using config {}", configFile);

    final ShuffleServer shuffleServer = new ShuffleServer(configFile);
    shuffleServer.start();

    shuffleServer.blockUntilShutdown();
  }

  public void start() throws Exception {
    registerHeartBeat.startHeartBeat();
    jettyServer.start();
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("*** shutting down gRPC server since JVM is shutting down");
        try {
          stopServer();
        } catch (Exception e) {
          LOG.error(e.getMessage());
        }
        LOG.info("*** server shut down");
      }
    });
    LOG.info("Shuffle server start successfully!");
  }

  public void stopServer() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
    if (registerHeartBeat != null) {
      registerHeartBeat.shutdown();
    }
    if (multiStorageManager != null) {
      multiStorageManager.stop();
    }
    server.stop();
  }

  private void initialization() throws Exception {
    ip = RssUtils.getHostIp();
    if (ip == null) {
      throw new RuntimeException("Couldn't acquire host Ip");
    }
    port = shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT);
    id = ip + "-" + port;
    LOG.info("Start to initialize server {}", id);
    if (shuffleServerConf.getBoolean(ShuffleServerConf.RSS_USE_MULTI_STORAGE)) {
      if (!StorageType.LOCALFILE.toString().equals(shuffleServerConf.get(RssBaseConf.RSS_STORAGE_TYPE))) {
        throw new IllegalArgumentException("Only StorageType LOCALFILE support multiStorage function");
      }
      multiStorageManager = new MultiStorageManager(shuffleServerConf, id);
      multiStorageManager.start();
    }
    registerHeartBeat = new RegisterHeartBeat(this);
    shuffleFlushManager = new ShuffleFlushManager(shuffleServerConf, id, this, multiStorageManager);
    shuffleBufferManager = new ShuffleBufferManager(shuffleServerConf, shuffleFlushManager);
    shuffleTaskManager = new ShuffleTaskManager(shuffleServerConf, shuffleFlushManager,
      shuffleBufferManager, multiStorageManager);

    RemoteServerFactory shuffleServerFactory = new RemoteServerFactory(this);
    server = shuffleServerFactory.getServer();
    jettyServer = new JettyServer(shuffleServerConf);

    registerMetrics();
    addServlet(jettyServer);
  }

  private void registerMetrics() {
    LOG.info("Register metrics");
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    ShuffleServerMetrics.register(shuffleServerCollectorRegistry);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry);
  }

  private void addServlet(JettyServer jettyServer) {
    LOG.info("Add metrics servlet");
    jettyServer.addServlet(
        new CommonMetricsServlet(ShuffleServerMetrics.getCollectorRegistry()),
        "/metrics/server");
    jettyServer.addServlet(
        new CommonMetricsServlet(JvmMetrics.getCollectorRegistry()),
        "/metrics/jvm");
    jettyServer.addServlet(
        new CommonMetricsServlet(ShuffleServerMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/server");
    jettyServer.addServlet(
        new CommonMetricsServlet(JvmMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/jvm");
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    server.blockUntilShutdown();
  }

  public String getIp() {
    return this.ip;
  }

  public String getId() {
    return this.id;
  }

  public int getPort() {
    return this.port;
  }

  public ShuffleServerConf getShuffleServerConf() {
    return this.shuffleServerConf;
  }

  public ServerInterface getServer() {
    return server;
  }

  public void setServer(ServerInterface server) {
    this.server = server;
  }

  public ShuffleTaskManager getShuffleTaskManager() {
    return shuffleTaskManager;
  }

  public ShuffleFlushManager getShuffleFlushManager() {
    return shuffleFlushManager;
  }

  public long getUsedMemory() {
    return shuffleBufferManager.getUsedMemory();
  }

  public long getPreAllocatedMemory() {
    return shuffleBufferManager.getPreAllocatedSize();
  }

  public long getAvailableMemory() {
    return shuffleBufferManager.getCapacity() - shuffleBufferManager.getPreAllocatedSize();
  }

  public int getEventNumInFlush() {
    return shuffleFlushManager.getEventNumInFlush();
  }

  public ShuffleBufferManager getShuffleBufferManager() {
    return shuffleBufferManager;
  }

  public MultiStorageManager getMultiStorageManager() {
    return multiStorageManager;
  }
}
