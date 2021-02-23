package com.tencent.rss.client.impl.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssGetShuffleAssignmentsRequest;
import com.tencent.rss.client.request.RssSendHeartBeatRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAppHeartBeatResponse;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.client.response.RssSendHeartBeatResponse;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.CoordinatorServerGrpc.CoordinatorServerBlockingStub;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.AppHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.AppHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse;
import com.tencent.rss.proto.RssProtos.PartitionRangeAssignment;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.proto.RssProtos.StatusCode;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorGrpcClient extends GrpcClient implements CoordinatorClient {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcClient.class);
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

  public GetShuffleServerListResponse getShuffleServerList() {
    return blockingStub.getShuffleServerList(Empty.newBuilder().build());
  }

  public ShuffleServerHeartBeatResponse doSendHeartBeat(
      String id, String ip, int port, int score, long timeout) {
    ShuffleServerId serverId =
        ShuffleServerId.newBuilder().setId(id).setIp(ip).setPort(port).build();
    ShuffleServerHeartBeatRequest request = ShuffleServerHeartBeatRequest
        .newBuilder().setServerId(serverId).setScore(score).build();

    StatusCode status;
    ShuffleServerHeartBeatResponse response = null;

    try {
      response = blockingStub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).heartbeat(request);
      status = response.getStatus();
    } catch (StatusRuntimeException e) {
      LOG.error(e.getMessage());
      status = StatusCode.TIMEOUT;
    } catch (Exception e) {
      LOG.error(e.getMessage());
      status = StatusCode.INTERNAL_ERROR;
    }

    if (response == null) {
      response = ShuffleServerHeartBeatResponse.newBuilder().setStatus(status).build();
    }

    if (status != StatusCode.SUCCESS) {
      LOG.error("Fail to send heartbeat to {}:{} {}", host, port, status);
    }

    return response;
  }

  public RssProtos.GetShuffleAssignmentsResponse doGetShuffleAssignments(
      String appId, int shuffleId, int numMaps, int partitionsPerServer) {

    RssProtos.GetShuffleServerRequest getServerRequest = RssProtos.GetShuffleServerRequest.newBuilder()
        .setApplicationId(appId)
        .setShuffleId(shuffleId)
        .setPartitionNum(numMaps)
        .setPartitionPerServer(partitionsPerServer)
        .build();

    return blockingStub.getShuffleAssignments(getServerRequest);
  }

  @Override
  public RssSendHeartBeatResponse sendHeartBeat(RssSendHeartBeatRequest request) {
    ShuffleServerHeartBeatResponse rpcResponse = doSendHeartBeat(
        request.getShuffleServerId(),
        request.getShuffleServerIp(),
        request.getShuffleServerPort(),
        request.getPercent(),
        request.getTimeout());

    RssSendHeartBeatResponse response;
    StatusCode statusCode = rpcResponse.getStatus();
    switch (statusCode) {
      case SUCCESS:
        response = new RssSendHeartBeatResponse(ResponseStatusCode.SUCCESS);
        response.setAppIds(Sets.newHashSet(rpcResponse.getAppIdList()));
        break;
      case TIMEOUT:
        response = new RssSendHeartBeatResponse(ResponseStatusCode.TIMEOUT);
        break;
      default:
        response = new RssSendHeartBeatResponse(ResponseStatusCode.INTERNAL_ERROR);
    }
    return response;
  }

  @Override
  public RssAppHeartBeatResponse sendAppHeartBeat(RssAppHeartBeatRequest request) {
    AppHeartBeatRequest rpcRequest = AppHeartBeatRequest.newBuilder().setAppId(request.getAppId()).build();
    AppHeartBeatResponse rpcResponse = blockingStub.appHeartbeat(rpcRequest);

    RssAppHeartBeatResponse response;
    StatusCode statusCode = rpcResponse.getStatus();
    switch (statusCode) {
      case SUCCESS:
        response = new RssAppHeartBeatResponse(ResponseStatusCode.SUCCESS);
        break;
      default:
        response = new RssAppHeartBeatResponse(ResponseStatusCode.INTERNAL_ERROR);
    }
    return response;
  }

  @Override
  public RssGetShuffleAssignmentsResponse getShuffleAssignments(RssGetShuffleAssignmentsRequest request) {
    RssProtos.GetShuffleAssignmentsResponse rpcResponse = doGetShuffleAssignments(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionNum(),
        request.getPartitionsPerServer());

    RssGetShuffleAssignmentsResponse response;
    StatusCode statusCode = rpcResponse.getStatus();
    switch (statusCode) {
      case SUCCESS:
        response = new RssGetShuffleAssignmentsResponse(ResponseStatusCode.SUCCESS);
        // get all register info according to coordinator's response
        List<ShuffleRegisterInfo> shuffleRegisterInfoList = getShuffleRegisterInfoList(rpcResponse);
        Map<Integer, List<ShuffleServerInfo>> partitionToServers = getPartitionToServers(rpcResponse);
        Set<ShuffleServerInfo> shuffleServersForResult = getServersForResult(rpcResponse);
        response.setRegisterInfoList(shuffleRegisterInfoList);
        response.setPartitionToServers(partitionToServers);
        response.setShuffleServersForResult(shuffleServersForResult);
        LOG.info("Successfully get shuffle assignments from coordinator, "
            + shuffleRegisterInfoList + ", " + partitionToServers);
        break;
      case TIMEOUT:
        response = new RssGetShuffleAssignmentsResponse(ResponseStatusCode.TIMEOUT);
        break;
      default:
        response = new RssGetShuffleAssignmentsResponse(ResponseStatusCode.INTERNAL_ERROR);
    }

    return response;
  }

  // transform [startPartition, endPartition] -> [server1, server2] to
  // {partition1 -> [server1, server2], partition2 - > [server1, server2]}
  @VisibleForTesting
  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers(
      RssProtos.GetShuffleAssignmentsResponse response) {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    List<PartitionRangeAssignment> assigns = response.getAssignmentsList();
    for (PartitionRangeAssignment partitionRangeAssignment : assigns) {
      final int startPartition = partitionRangeAssignment.getStartPartition();
      final int endPartition = partitionRangeAssignment.getEndPartition();
      final List<ShuffleServerInfo> shuffleServerInfos = partitionRangeAssignment
          .getServerList()
          .parallelStream()
          .map(ss -> new ShuffleServerInfo(ss.getId(), ss.getIp(), ss.getPort()))
          .collect(Collectors.toList());
      for (int i = startPartition; i <= endPartition; i++) {
        partitionToServers.put(i, shuffleServerInfos);
      }
    }
    if (partitionToServers.isEmpty()) {
      throw new RuntimeException("Empty assignment to Shuffle Server");
    }
    return partitionToServers;
  }

  // get all ShuffleRegisterInfo with [shuffleServer, startPartitionId, endPartitionId]
  @VisibleForTesting
  public List<ShuffleRegisterInfo> getShuffleRegisterInfoList(
      RssProtos.GetShuffleAssignmentsResponse response) {
    // make the list thread safe, or get incorrect result in parallelStream
    List<ShuffleRegisterInfo> shuffleRegisterInfoList = Collections.synchronizedList(Lists.newArrayList());
    List<PartitionRangeAssignment> assigns = response.getAssignmentsList();
    for (PartitionRangeAssignment assign : assigns) {
      List<ShuffleServerId> shuffleServerIds = assign.getServerList();
      final int startPartition = assign.getStartPartition();
      final int endPartition = assign.getEndPartition();
      if (shuffleServerIds != null) {
        shuffleServerIds.parallelStream().forEach(ssi -> {
              ShuffleServerInfo shuffleServerInfo =
                  new ShuffleServerInfo(ssi.getId(), ssi.getIp(), ssi.getPort());
              ShuffleRegisterInfo shuffleRegisterInfo = new ShuffleRegisterInfo(shuffleServerInfo,
                  startPartition, endPartition);
              shuffleRegisterInfoList.add(shuffleRegisterInfo);
            }
        );
      }
    }
    return shuffleRegisterInfoList;
  }

  private Set<ShuffleServerInfo> getServersForResult(RssProtos.GetShuffleAssignmentsResponse response) {
    List<ShuffleServerId> shuffleServerIds = response.getServerForResultList();
    if (shuffleServerIds == null || shuffleServerIds.isEmpty()) {
      throw new RuntimeException("Failed to get shuffle server for report shuffle result");
    }
    Set<ShuffleServerInfo> shuffleServerInfoSet = Sets.newHashSet();
    for (ShuffleServerId ssi : shuffleServerIds) {
      shuffleServerInfoSet.add(new ShuffleServerInfo(ssi.getId(), ssi.getIp(), ssi.getPort()));
    }
    return shuffleServerInfoSet;
  }
}
