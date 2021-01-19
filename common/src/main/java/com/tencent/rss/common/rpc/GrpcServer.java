package com.tencent.rss.common.rpc;

import com.tencent.rss.common.config.RssBaseConf;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcServer implements ServerInterface {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

  private final Server server;
  private final int port;

  public GrpcServer(RssBaseConf conf, BindableService service) {
    this.port = conf.getInteger(RssBaseConf.SERVER_PORT);
    int maxInboundMessageSize = conf.getInteger(RssBaseConf.RPC_MESSAGE_MAX_SIZE);
    this.server = ServerBuilder
        .forPort(port)
        .addService(service)
        .maxInboundMessageSize(maxInboundMessageSize)
        .build();
  }

  public GrpcServer(Server server) {
    this.server = server;
    this.port = -1;
  }

  public void start() throws IOException {
    server.start();
    LOG.info("Grpc server started, listening on {}.", port);
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

}
