package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.common.CoordinatorGrpcClient;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class RegisterHeartBeat {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterHeartBeat.class);

  private boolean isRegistered;
  private int heartBeatInitialDelay;
  private int heartBeatInterval;
  private int failedHeartBeatCount;
  private int maxHeartBeatRetryCount;

  private CoordinatorGrpcClient rpcClient;

  public RegisterHeartBeat(CoordinatorGrpcClient client) {
    this.rpcClient = client;
    isRegistered = false;
  }

  public boolean register(String id, String ip, int port) {
//    ServerRegisterRequest request = null;
//    ServerRegisterResponse response = rpcClient.register(id, ip, port);
    StatusCode status = StatusCode.SUCCESS;
    // TODO: extract info from response

    isRegistered = status == StatusCode.SUCCESS;
    return isRegistered;
  }

  public void startHeartBeat(String id, String ip, int port) {
    ScheduledExecutorService service = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setDaemon(true);
          return t;
        }
      });

    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        sendHeartBeat(id, ip, port);
      }
    };

    int delay = ThreadLocalRandom.current().nextInt(0, heartBeatInitialDelay);
    service.scheduleAtFixedRate(runnable, delay, heartBeatInterval, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  boolean sendHeartBeat(String id, String ip, int port) {
    ShuffleServerHeartBeatResponse response = rpcClient.sendHeartBeat(
      id, ip, port, BufferManager.instance().getAvailableCount());
    StatusCode status = response.getStatus();

    if (status != StatusCode.SUCCESS) {
      failedHeartBeatCount++;
    } else {
      failedHeartBeatCount = 0;
      isRegistered = true;
    }

    if (failedHeartBeatCount >= maxHeartBeatRetryCount) {
      LOGGER.error(
        "Failed heartbeat count exceed {}",
        maxHeartBeatRetryCount);
      isRegistered = false;

      // TODO: add HA
    }

    return status == StatusCode.SUCCESS;
  }

  @VisibleForTesting
  int getFailedHeartBeatCount() {
    return this.failedHeartBeatCount;
  }

  @VisibleForTesting
  boolean getIsRegistered() {
    return this.isRegistered;
  }

  @VisibleForTesting
  void setIsRegistered(boolean isRegistered) {
    this.isRegistered = isRegistered;
  }

  @VisibleForTesting
  void setMaxHeartBeatRetryCount(int maxHeartBeatRetryCount) {
    this.maxHeartBeatRetryCount = maxHeartBeatRetryCount;
  }
}
