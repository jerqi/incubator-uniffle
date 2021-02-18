package com.tencent.rss.server;

import com.tencent.rss.common.Arguments;
import com.tencent.rss.common.metrics.JvmMetrics;
import com.tencent.rss.common.rpc.ServerInterface;
import com.tencent.rss.common.web.JettyServer;
import com.tencent.rss.common.web.MetricsServlet;
import io.prometheus.client.CollectorRegistry;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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

  public ShuffleServer(ShuffleServerConf shuffleServerConf) throws UnknownHostException, FileNotFoundException {
    this.shuffleServerConf = shuffleServerConf;
    initialization();
  }

  public ShuffleServer(String configFile) throws FileNotFoundException, IllegalStateException, UnknownHostException {
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
  }

  public void stopServer() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
    server.stop();
  }

  private void initialization() throws UnknownHostException, FileNotFoundException {
    ip = InetAddress.getLocalHost().getHostAddress();
    port = shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT);
    id = ip + "-" + port;
    registerHeartBeat = new RegisterHeartBeat(this);
    shuffleFlushManager = new ShuffleFlushManager(shuffleServerConf, id, this);
    shuffleBufferManager = new ShuffleBufferManager(shuffleServerConf, shuffleFlushManager);

    shuffleTaskManager = new ShuffleTaskManager(shuffleServerConf, shuffleFlushManager, shuffleBufferManager);

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
    jettyServer.addServlet(new MetricsServlet(ShuffleServerMetrics.getCollectorRegistry()), "/metrics/server");
    jettyServer.addServlet(new MetricsServlet(JvmMetrics.getCollectorRegistry()), "/metrics/jvm");
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

  // TODO: add score calculation strategy
  public int calcScore() {
    return 100 - shuffleBufferManager.getBufferUsedPercent();
  }

  public ShuffleBufferManager getShuffleBufferManager() {
    return shuffleBufferManager;
  }
}
