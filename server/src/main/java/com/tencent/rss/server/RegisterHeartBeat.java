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
import java.util.concurrent.TimeUnit;

public class RegisterHeartBeat {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterHeartBeat.class);
  private final String ip;
  private final int port;

  private final long heartBeatInitialDelay;
  private final long heartBeatInterval;
  private final ShuffleServer shuffleServer;
  private long heartBeatTimeout;
  private CoordinatorGrpcClient rpcClient;

  private boolean isRegistered;
  private int failedHeartBeatCount;
  private int maxHeartBeatRetry;


  public RegisterHeartBeat(ShuffleServer shuffleServer) {
    ShuffleServerConf conf = shuffleServer.getShuffleServerConf();
    this.ip = conf.getString(ShuffleServerConf.COORDINATOR_IP);
    this.port = conf.getInteger(ShuffleServerConf.COORDINATOR_PORT);
    this.heartBeatInitialDelay = conf.getLong(ShuffleServerConf.HEARTBEAT_DELAY);
    this.heartBeatInterval = conf.getLong(ShuffleServerConf.HEARTBEAT_INTERVAL);
    this.heartBeatTimeout = conf.getLong(ShuffleServerConf.HEARTBEAT_TIMEOUT);
    this.maxHeartBeatRetry = conf.getInteger(ShuffleServerConf.HEARTBEAT_MAX_FAILURE);
    this.rpcClient = new CoordinatorGrpcClient(ip, port);
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
        sendHeartBeat(
            shuffleServer.getId(),
            shuffleServer.getIp(),
            shuffleServer.getPort(),
            shuffleServer.getBufferManager().getBufferUsedPercent());
      }
    };

    service.scheduleAtFixedRate(runnable, heartBeatInitialDelay, heartBeatInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  boolean sendHeartBeat(String id, String ip, int port, int percent) {
    LOGGER.debug("Start to send heartbeat " + System.currentTimeMillis());
    ShuffleServerHeartBeatResponse response = rpcClient.sendHeartBeat(id, ip, port, percent, heartBeatTimeout);
    StatusCode status = response.getStatus();

    if (status != StatusCode.SUCCESS) {
      LOGGER.error("Can't send heartbeat to Coordinator");
      failedHeartBeatCount++;
    } else {
      LOGGER.debug("Success to send heartbeat");
      failedHeartBeatCount = 0;
      isRegistered = true;
    }

    if (failedHeartBeatCount >= maxHeartBeatRetry) {
      LOGGER.error(
          "Failed heartbeat count {} exceed {}",
          failedHeartBeatCount, maxHeartBeatRetry);
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
  void setMaxHeartBeatRetry(int maxHeartBeatRetry) {
    this.maxHeartBeatRetry = maxHeartBeatRetry;
  }

  @VisibleForTesting
  void setHeartBeatTimeout(long timeout) {
    this.heartBeatTimeout = timeout;
  }
}
