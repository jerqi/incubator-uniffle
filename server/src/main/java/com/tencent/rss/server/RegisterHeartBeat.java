package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.factory.CoordinatorClientFactory;
import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.client.request.RssSendHeartBeatRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssSendHeartBeatResponse;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
  }

  public RegisterHeartBeat(ShuffleServer shuffleServer, CoordinatorGrpcClient client) {
    this(shuffleServer);
    this.rpcClient = client;
  }

  public void startHeartBeat() {
    LOGGER.info("Start heartbeat to coordinator {}:{} after {}ms and interval is {}ms",
        ip, port, heartBeatInitialDelay, heartBeatInterval);
    Runnable runnable = () -> sendHeartBeat(
        shuffleServer.getId(),
        shuffleServer.getIp(),
        shuffleServer.getPort(),
        shuffleServer.getUsedMemory(),
        shuffleServer.getPreAllocatedMemory(),
        shuffleServer.getAvailableMemory(),
        shuffleServer.getEventNumInFlush());

    service = Executors.newSingleThreadScheduledExecutor();
    service.scheduleAtFixedRate(runnable, heartBeatInitialDelay, heartBeatInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  boolean sendHeartBeat(String id, String ip, int port, long usedMemory,
      long preAllocatedMemory, long availableMemory, int eventNumInFlush) {
    boolean sendSuccessfully = false;
    RssSendHeartBeatRequest request = new RssSendHeartBeatRequest(
        id, ip, port, usedMemory, preAllocatedMemory, availableMemory, eventNumInFlush, heartBeatTimeout);
    RssSendHeartBeatResponse response = rpcClient.sendHeartBeat(request);
    ResponseStatusCode status = response.getStatusCode();

    if (status != ResponseStatusCode.SUCCESS) {
      LOGGER.error("Can't send heartbeat to coordinator");
      failedHeartBeatCount++;
    } else {
      LOGGER.debug("Get heartbeat response with appIds " + response.getAppIds());
      failedHeartBeatCount = 0;
      sendSuccessfully = true;
      checkResourceStatus(response.getAppIds());
    }

    if (failedHeartBeatCount >= maxHeartBeatRetry) {
      LOGGER.error(
          "Failed heartbeat count {} exceed {}",
          failedHeartBeatCount, maxHeartBeatRetry);
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
  void setMaxHeartBeatRetry(int maxHeartBeatRetry) {
    this.maxHeartBeatRetry = maxHeartBeatRetry;
  }

  @VisibleForTesting
  void setHeartBeatTimeout(long timeout) {
    this.heartBeatTimeout = timeout;
  }
}
