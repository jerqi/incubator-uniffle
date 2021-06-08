package com.tencent.rss.client.impl.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class GrpcClient {

  private static final Logger logger = LoggerFactory.getLogger(GrpcClient.class);
  protected String host;
  protected int port;
  protected boolean usePlaintext;
  protected int maxRetryAttempts;
  protected ManagedChannel channel;

  protected GrpcClient(String host, int port, int maxRetryAttempts, boolean usePlaintext) {
    this.host = host;
    this.port = port;
    this.maxRetryAttempts = maxRetryAttempts;
    this.usePlaintext = usePlaintext;

    // build channel
    ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(host, port);

    if (usePlaintext) {
      channelBuilder.usePlaintext();
    }

    if (maxRetryAttempts > 0) {
      channelBuilder.enableRetry().maxRetryAttempts(maxRetryAttempts);
    }
    channelBuilder.maxInboundMessageSize(Integer.MAX_VALUE);

    channel = channelBuilder.build();
  }

  protected GrpcClient(ManagedChannel channel) {
    this.channel = channel;
  }

  public void close() {
    try {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error("Can't close GRPC client to " + host + ":" + port);
    }
  }

}
