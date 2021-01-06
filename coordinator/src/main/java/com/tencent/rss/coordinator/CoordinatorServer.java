package com.tencent.rss.coordinator;

import com.tencent.rss.common.Arguments;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The main entrance of coordinator service
 */
public class CoordinatorServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorServer.class);

  private final CoordinatorConf coordinatorConf;
  private Server server;

  public CoordinatorServer(CoordinatorConf coordinatorConf) {
    this.coordinatorConf = coordinatorConf;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOGGER.info("Start to init shuffle server using config {}", configFile);

    // Load configuration from config files
    final CoordinatorConf coordinatorConf = new CoordinatorConf(configFile);

    // Start the coordinator service
    final CoordinatorServer server = new CoordinatorServer(coordinatorConf);
    server.start();
    server.blockUntilShutdown();
  }

  public void start() throws IOException {
    /* The port on which the server should run */
    final int port = coordinatorConf.getCoordinatorServicePort();

    server = ServerBuilder.forPort(port)
        .addService(new CoordinatorServiceImp(coordinatorConf))
        .build()
        .start();
    LOGGER.info("Coordinator server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.info("Shutting down coordinator server");
      try {
        CoordinatorServer.this.stop();
      } catch (InterruptedException e) {
        e.printStackTrace(System.err);
      }
      LOGGER.info("Coordinator server shut down");
    }));
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }
}
