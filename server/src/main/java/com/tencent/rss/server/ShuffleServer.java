package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class ShuffleServer {

  private static final Logger logger = LoggerFactory.getLogger(ShuffleServer.class);

  private int port;
  private Server server;

  public ShuffleServer(ShuffleServerConf conf) {
    this(conf.getInteger(ShuffleServerConf.SERVICE_PORT));
  }

  public ShuffleServer(int port) {
    this(ServerBuilder.forPort(port), port);
  }

  public ShuffleServer(ServerBuilder<?> serverBuilder, int port) {
    this.port = port;
    server = serverBuilder.addService(new RemoteShuffleService()).build();
  }

  /**
   * Main launches the server from the command line.
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);

    ShuffleServerConf serverConf = new ShuffleServerConf();
    if (!serverConf.loadConfFromFile(arguments.getConfigFile())) {
      System.exit(1);
    }

    if (!ShuffleTaskManager.instance().init(serverConf)) {
      System.exit(1);
    }

    if (!BufferManager.instance().init(serverConf)) {
      System.exit(1);
    }

    final ShuffleServer server = new ShuffleServer(serverConf);
    server.start();
    server.blockUntilShutdown();
  }

  public void start() throws IOException {
    server.start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        try {
          ShuffleServer.this.stop();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        System.err.println("*** server shut down");
      }
    });
  }

  @VisibleForTesting
  void stop() throws InterruptedException {
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
