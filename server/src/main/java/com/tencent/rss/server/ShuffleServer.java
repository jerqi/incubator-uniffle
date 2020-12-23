package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.common.metrics.JvmMetrics;
import com.tencent.rss.common.web.JettyServer;
import com.tencent.rss.common.web.MetricsServlet;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class ShuffleServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleServer.class);
  private final RegisterHeartBeat registerHeartBeat;
  private final BufferManager bufferManager;
  private final String id;
  private final String ip;
  private final int port;
  private final ShuffleServerConf shuffleServerConf;
  private final JettyServer jettyServer;
  private ShuffleTaskManager shuffleTaskManager;
  private Server grpcServer;

  public ShuffleServer(String configFile) throws FileNotFoundException, IllegalStateException, UnknownHostException {
    this.shuffleServerConf = new ShuffleServerConf(configFile);
    this.ip = InetAddress.getLocalHost().getHostAddress();
    this.port = shuffleServerConf.getInteger(ShuffleServerConf.SERVICE_PORT);
    this.id = ip + "-" + port;

    this.registerHeartBeat = new RegisterHeartBeat(this);
    this.bufferManager = new BufferManager(shuffleServerConf);
    this.shuffleTaskManager = new ShuffleTaskManager(shuffleServerConf, bufferManager, id);

    this.grpcServer = ServerBuilder.forPort(port).addService(new RemoteShuffleService(this)).build();
    this.jettyServer = new JettyServer(configFile);
  }

  public static void registerMetrics() {
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    ShuffleServerMetrics.register(shuffleServerCollectorRegistry);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry);
  }

  public static void addServlet(JettyServer jettyServer) {
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    ShuffleServerMetrics.register(shuffleServerCollectorRegistry);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry);
    jettyServer.addServlet(new MetricsServlet(ShuffleServerMetrics.getCollectorRegistry()), "/metrics/server");
    jettyServer.addServlet(new MetricsServlet(JvmMetrics.getCollectorRegistry()), "/metrics/jvm");
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);

    final ShuffleServer shuffleServer = new ShuffleServer(arguments.getConfigFile());

    registerMetrics();
    addServlet(shuffleServer.getJettyServer());
    launch(shuffleServer);

    shuffleServer.blockUntilShutdown();
  }

  public static void launch(ShuffleServer shuffleServer) throws Exception {
    shuffleServer.getRegisterHeartBeat().startHeartBeat();
    shuffleServer.getJettyServer().start();
    shuffleServer.getGrpcServer().start();

    LOGGER.info("Grpc server started, listening on {}.", shuffleServer.getPort());

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          shuffleServer.stop();
        } catch (InterruptedException e) {
          LOGGER.error(e.getMessage());
        }
        LOGGER.info("*** server shut down");

      }
    });
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

  public int getAvailabelBufferNum() {
    return bufferManager.getAvailableCount();
  }

  public RegisterHeartBeat getRegisterHeartBeat() {
    return registerHeartBeat;
  }

  public Server getGrpcServer() {
    return grpcServer;
  }

  public void setGrpcServer(Server grpcServer) {
    this.grpcServer = grpcServer;
  }

  public JettyServer getJettyServer() {
    return this.jettyServer;
  }

  public ShuffleTaskManager getShuffleTaskManager() {
    return this.shuffleTaskManager;
  }

  public void setShuffleTaskManager(ShuffleTaskManager shuffleTaskManager) {
    this.shuffleTaskManager = shuffleTaskManager;
  }

  @VisibleForTesting
  void stop() throws InterruptedException {
    if (grpcServer != null) {
      grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (grpcServer != null) {
      grpcServer.awaitTermination();
    }
  }

}
