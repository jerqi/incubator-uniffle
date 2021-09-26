package com.tencent.rss.common.rpc;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.ExitUtils;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcServer implements ServerInterface {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

  private final Server server;
  private final int port;

  public GrpcServer(RssBaseConf conf, BindableService service) {
    this.port = conf.getInteger(RssBaseConf.RPC_SERVER_PORT);
    int maxInboundMessageSize = conf.getInteger(RssBaseConf.RPC_MESSAGE_MAX_SIZE);
    int rpcExecutorSize = conf.getInteger(RssBaseConf.RPC_EXECUTOR_SIZE);
    ExecutorService pool = new ThreadPoolExecutor(
        rpcExecutorSize,
        rpcExecutorSize * 2,
        10,
        TimeUnit.MINUTES,
        Queues.newLinkedBlockingQueue(Integer.MAX_VALUE),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Grpc-%d").build()
    );

    this.server = ServerBuilder
        .forPort(port)
        .addService(service)
        .executor(pool)
        .maxInboundMessageSize(maxInboundMessageSize)
        .build();
  }

  public void start() throws IOException {
    try {
      server.start();
    } catch (IOException e) {
      ExitUtils.terminate(1, "Fail to start grpc server", e, LOG);
    }
    LOG.info("Grpc server started, listening on {}.", port);
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

}
