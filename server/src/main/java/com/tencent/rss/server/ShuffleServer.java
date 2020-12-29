package com.tencent.rss.server;

import com.tencent.rss.common.metrics.JvmMetrics;
import com.tencent.rss.common.web.JettyServer;
import com.tencent.rss.common.web.MetricsServlet;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.prometheus.client.CollectorRegistry;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class ShuffleServer {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServer.class);
  private RegisterHeartBeat registerHeartBeat;
  private BufferManager bufferManager;
  private String id;
  private String ip;
  private int port;
  private ShuffleServerConf shuffleServerConf;
  private JettyServer jettyServer;
  private ShuffleTaskManager shuffleTaskManager;
  private Server grpcServer;

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

    final ShuffleServer shuffleServer = new ShuffleServer(arguments.getConfigFile());
//    registerMetrics();
//    addServlet(shuffleServer.getJettyServer());
    shuffleServer.start();

    shuffleServer.blockUntilShutdown();
  }

//  private void registerMetrics() {
//    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
//    ShuffleServerMetrics.register(shuffleServerCollectorRegistry);
//    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
//    JvmMetrics.register(jvmCollectorRegistry);
//  }

  private void addServlet(JettyServer jettyServer) {
    CollectorRegistry shuffleServerCollectorRegistry = new CollectorRegistry(true);
    ShuffleServerMetrics.register(shuffleServerCollectorRegistry);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    JvmMetrics.register(jvmCollectorRegistry);
    jettyServer.addServlet(new MetricsServlet(ShuffleServerMetrics.getCollectorRegistry()), "/metrics/server");
    jettyServer.addServlet(new MetricsServlet(JvmMetrics.getCollectorRegistry()), "/metrics/jvm");
  }

  private void initialization() throws UnknownHostException, FileNotFoundException {
    ip = InetAddress.getLocalHost().getHostAddress();
    port = shuffleServerConf.getInteger(ShuffleServerConf.SERVICE_PORT);
    id = ip + "-" + port;
    registerHeartBeat = new RegisterHeartBeat(this);
    bufferManager = new BufferManager(shuffleServerConf);
    shuffleTaskManager = new ShuffleTaskManager(shuffleServerConf, bufferManager, id);
    grpcServer = ServerBuilder.forPort(port).addService(new RemoteShuffleService(this)).build();
    jettyServer = new JettyServer(shuffleServerConf);

//    registerMetrics();
    addServlet(jettyServer);
  }

  public void start() throws Exception {
    registerHeartBeat.startHeartBeat();
    jettyServer.start();
    grpcServer.start();

    LOG.info("Grpc server started, listening on {}.", port);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          stopServer();
        } catch (Exception e) {
          LOG.error(e.getMessage());
        }
        LOG.info("*** server shut down");
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

  public void stopServer() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
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
