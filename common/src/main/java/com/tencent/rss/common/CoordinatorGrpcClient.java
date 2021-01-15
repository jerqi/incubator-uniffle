package com.tencent.rss.common;

import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.CoordinatorServerGrpc.CoordinatorServerBlockingStub;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.proto.RssProtos.StatusCode;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class CoordinatorGrpcClient extends GrpcClient {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatorGrpcClient.class);
  private CoordinatorServerBlockingStub blockingStub;

  public CoordinatorGrpcClient(String host, int port) {
    this(host, port, 3);
  }

  public CoordinatorGrpcClient(String host, int port, int maxRetryAttempts) {
    this(host, port, maxRetryAttempts, true);
  }

  public CoordinatorGrpcClient(String host, int port, int maxRetryAttempts, boolean usePlaintext) {
    super(host, port, maxRetryAttempts, usePlaintext);
    blockingStub = CoordinatorServerGrpc.newBlockingStub(channel);
  }

  public CoordinatorGrpcClient(ManagedChannel channel) {
    super(channel);
    blockingStub = CoordinatorServerGrpc.newBlockingStub(channel);
  }

  public ShuffleServerHeartBeatResponse sendHeartBeat(
      String id, String ip, int port, int percent, long timeout) {
    ShuffleServerId serverId =
        ShuffleServerId.newBuilder().setId(id).setIp(ip).setPort(port).build();
    ShuffleServerHeartBeatRequest request = ShuffleServerHeartBeatRequest
        .newBuilder().setServerId(serverId).setBufferUsedPercent(percent).build();

    StatusCode status;
    ShuffleServerHeartBeatResponse response = null;

    try {
      response = blockingStub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).heartbeat(request);
      status = response.getStatus();
    } catch (StatusRuntimeException e) {
      logger.error(e.getMessage());
      status = StatusCode.TIMEOUT;
    } catch (Exception e) {
      logger.error(e.getMessage());
      status = StatusCode.INTERNAL_ERROR;
    }

    if (response == null) {
      response = ShuffleServerHeartBeatResponse.newBuilder().setStatus(status).build();
    }

    if (status != StatusCode.SUCCESS) {
      logger.error("Fail to send heartbeat to {}:{} {}", host, port, status);
    }

    return response;
  }

  public RssProtos.GetShuffleAssignmentsResponse getShuffleAssignments(
      String appId, int shuffleId, int numMaps, int partitionsPerServer) {

    RssProtos.GetShuffleServerRequest getServerRequest = RssProtos.GetShuffleServerRequest.newBuilder()
        .setApplicationId(appId)
        .setShuffleId(shuffleId)
        .setPartitionNum(numMaps)
        .setPartitionPerServer(partitionsPerServer)
        .build();

    return blockingStub.getShuffleAssignments(getServerRequest);
  }
}
