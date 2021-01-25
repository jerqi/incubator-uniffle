package com.tencent.rss.client.impl.grpc;

import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.proto.RssProtos;
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

  protected ResponseStatusCode toResponseStatusCode(RssProtos.StatusCode code) {
    switch (code) {
      case SUCCESS:
        return ResponseStatusCode.SUCCESS;
      case DOUBLE_REGISTER:
        return ResponseStatusCode.DOUBLE_REGISTER;
      case NO_BUFFER:
        return ResponseStatusCode.NO_BUFFER;
      case INVALID_STORAGE:
        return ResponseStatusCode.INVALID_STORAGE;
      case NO_REGISTER:
        return ResponseStatusCode.NO_REGISTER;
      case NO_PARTITION:
        return ResponseStatusCode.NO_PARTITION;
      case INTERNAL_ERROR:
        return ResponseStatusCode.INTERNAL_ERROR;
      case TIMEOUT:
        return ResponseStatusCode.TIMEOUT;
      default:
        return ResponseStatusCode.INTERNAL_ERROR;
    }
  }

  public void close() {
    try {
      channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error("Can't close GRPC client to " + host + ":" + port);
    }
  }
}
