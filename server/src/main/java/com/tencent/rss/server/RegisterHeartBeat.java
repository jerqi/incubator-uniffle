package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.client.request.RssSendHeartBeatRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssSendHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterHeartBeat {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterHeartBeat.class);
  private final String ip;
  private final int port;

  private final long heartBeatInitialDelay;
  private final long heartBeatInterval;
  private final ShuffleServer shuffleServer;
  private long heartBeatTimeout;
  private CoordinatorClient rpcClient;

  private boolean isRegistered;
  private int failedHeartBeatCount;
  private int maxHeartBeatRetry;
  private ScheduledExecutorService service;


  public RegisterHeartBeat(ShuffleServer shuffleServer) {
    ShuffleServerConf conf = shuffleServer.getShuffleServerConf();
    this.ip = conf.getString(ShuffleServerConf.RSS_COORDINATOR_IP);
    this.port = conf.getInteger(ShuffleServerConf.RSS_COORDINATOR_PORT);
    this.heartBeatInitialDelay = conf.getLong(ShuffleServerConf.SERVER_HEARTBEAT_DELAY);
    this.heartBeatInterval = conf.getLong(ShuffleServerConf.SERVER_HEARTBEAT_INTERVAL);
    this.heartBeatTimeout = conf.getLong(ShuffleServerConf.SERVER_HEARTBEAT_TIMEOUT);
    this.maxHeartBeatRetry = conf.getInteger(ShuffleServerConf.SERVER_HEARTBEAT_MAX_FAILURE);
    CoordinatorClientFactory factory =
        new CoordinatorClientFactory(conf.getString(ShuffleServerConf.RSS_CLIENT_TYPE));
    this.rpcClient = factory.createCoordinatorClient(ip, port);
    this.shuffleServer = shuffleServer;
    this.isRegistered = false;
  }

  public RegisterHeartBeat(ShuffleServer shuffleServer, CoordinatorGrpcClient client) {
    this(shuffleServer);
    this.rpcClient = client;
  }

  public boolean register(String id, String ip, int port) {
    StatusCode status = StatusCode.SUCCESS;
    // TODO: extract info from response

    isRegistered = status == StatusCode.SUCCESS;
    return isRegistered;
  }

  public void startHeartBeat() {
    LOGGER.info("Start heartbeat to coordinator {}:{} after {}ms and interval is {}ms",
        ip, port, heartBeatInitialDelay, heartBeatInterval);
    service = Executors
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
        sendHeartBeat(
            shuffleServer.getId(),
            shuffleServer.getIp(),
            shuffleServer.getPort(),
            shuffleServer.getUsedMemory(),
            shuffleServer.getPreAllocatedMemory(),
            shuffleServer.getAvailableMemory(),
            shuffleServer.getEventNumInFlush());
      }
    };

    service.scheduleAtFixedRate(runnable, heartBeatInitialDelay, heartBeatInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  boolean sendHeartBeat(String id, String ip, int port, long usedMemory,
      long preAllocatedMemory, long availableMemory, int eventNumInFlush) {
    LOGGER.debug("Start to send heartbeat " + System.currentTimeMillis());
    boolean sendSuccessfully = false;
    RssSendHeartBeatRequest request = new RssSendHeartBeatRequest(
        id, ip, port, usedMemory, preAllocatedMemory, availableMemory, eventNumInFlush, heartBeatTimeout);
    RssSendHeartBeatResponse response = rpcClient.sendHeartBeat(request);
    ResponseStatusCode status = response.getStatusCode();

    if (status != ResponseStatusCode.SUCCESS) {
      LOGGER.error("Can't send heartbeat to Coordinator");
      failedHeartBeatCount++;
    } else {
      LOGGER.debug("Success to send heartbeat");
      failedHeartBeatCount = 0;
      isRegistered = true;
      sendSuccessfully = true;
      checkResourceStatus(response.getAppIds());
    }

    if (failedHeartBeatCount >= maxHeartBeatRetry) {
      LOGGER.error(
          "Failed heartbeat count {} exceed {}",
          failedHeartBeatCount, maxHeartBeatRetry);
      isRegistered = false;
      // TODO: add HA
    }

    return sendSuccessfully;
  }

  private void checkResourceStatus(Set<String> appIds) {
    shuffleServer.getShuffleTaskManager().checkResourceStatus(appIds);
  }

  public void shutdown() {
    service.shutdown();
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
  void setMaxHeartBeatRetry(int maxHeartBeatRetry) {
    this.maxHeartBeatRetry = maxHeartBeatRetry;
  }

  @VisibleForTesting
  void setHeartBeatTimeout(long timeout) {
    this.heartBeatTimeout = timeout;
  }
}
