package com.tencent.rss.coordinator;

import com.tencent.rss.common.Arguments;
import com.tencent.rss.common.rpc.ServerInterface;
import com.tencent.rss.common.web.JettyServer;
import java.io.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * The main entrance of coordinator service
 */
public class CoordinatorServer {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorServer.class);

  private final CoordinatorConf coordinatorConf;
  private JettyServer jettyServer;
  private ServerInterface server;
  private ClusterManager clusterManager;
  private AssignmentStrategy assignmentStrategy;
  private ApplicationManager applicationManager;

  public CoordinatorServer(CoordinatorConf coordinatorConf) throws FileNotFoundException {
    this.applicationManager = new ApplicationManager(coordinatorConf);
    this.coordinatorConf = coordinatorConf;

    ClusterManagerFactory clusterManagerFactory =
        new ClusterManagerFactory(coordinatorConf);
    this.clusterManager = clusterManagerFactory.getClusterManager();

    AssignmentStrategyFactory assignmentStrategyFactory =
        new AssignmentStrategyFactory(coordinatorConf, clusterManager);
    this.assignmentStrategy = assignmentStrategyFactory.getAssignmentStrategy();

    CoordinatorRpcServerFactory coordinatorRpcServerFactory = new CoordinatorRpcServerFactory(this);
    server = coordinatorRpcServerFactory.getServer();
    jettyServer = new JettyServer(coordinatorConf);

  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOG.info("Start to init coordinator server using config {}", configFile);

    // Load configuration from config files
    final CoordinatorConf coordinatorConf = new CoordinatorConf(configFile);

    // Start the coordinator service
    final CoordinatorServer coordinatorServer = new CoordinatorServer(coordinatorConf);
    coordinatorServer.start();
    coordinatorServer.blockUntilShutdown();
  }

  public void start() throws Exception {
    server.start();
    jettyServer.start();

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

  public ClusterManager getClusterManager() {
    return clusterManager;
  }

  public AssignmentStrategy getAssignmentStrategy() {
    return assignmentStrategy;
  }

  public CoordinatorConf getCoordinatorConf() {
    return coordinatorConf;
  }

  public ApplicationManager getApplicationManager() {
    return applicationManager;
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    server.blockUntilShutdown();
  }
}
