package com.tencent.rss.coordinator;

import com.google.common.collect.Sets;
import com.google.protobuf.Empty;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.RssProtos.AppHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.AppHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.CheckServiceAvailableResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerNumResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleClientOpRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleClientOpResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.proto.RssProtos.StatusCode;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation class for services defined in protobuf
 */
public class CoordinatorGrpcService extends CoordinatorServerGrpc.CoordinatorServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorGrpcService.class);

  private final CoordinatorServer coordinatorServer;

  CoordinatorGrpcService(CoordinatorServer coordinatorServer) {
    this.coordinatorServer = coordinatorServer;
  }

  @Override
  public void getShuffleServerList(
      Empty request,
      StreamObserver<GetShuffleServerListResponse> responseObserver) {
    final GetShuffleServerListResponse response = GetShuffleServerListResponse
        .newBuilder()
        .addAllServers(
            coordinatorServer
                .getClusterManager()
                .list().stream()
                .map(ServerNode::convertToGrpcProto)
                .collect(Collectors.toList()))
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleServerNum(
      Empty request,
      StreamObserver<GetShuffleServerNumResponse> responseObserver) {
    final int num = coordinatorServer.getClusterManager().getNodesNum();
    final GetShuffleServerNumResponse response = GetShuffleServerNumResponse
        .newBuilder()
        .setNum(num)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleAssignments(
      GetShuffleServerRequest request,
      StreamObserver<GetShuffleAssignmentsResponse> responseObserver) {
    final String appId = request.getApplicationId();
    final int shuffleId = request.getShuffleId();
    final int partitionNum = request.getPartitionNum();
    final int partitionNumPerRange = request.getPartitionNumPerRange();
    final int replica = request.getDataReplica();
    final Set<String> requiredTags = Sets.newHashSet(request.getRequireTagsList());

    LOG.info("Request of getShuffleAssignments for appId[" + appId
        + "], shuffleId[" + shuffleId + "], partitionNum[" + partitionNum
        + "], partitionNumPerRange[" + partitionNumPerRange + "], replica[" + replica + "]");

    final PartitionRangeAssignment pra =
        coordinatorServer.getAssignmentStrategy().assign(partitionNum, partitionNumPerRange, replica, requiredTags);
    final List<ServerNode> serverNodes =
        coordinatorServer.getAssignmentStrategy().assignServersForResult(replica, requiredTags);
    final GetShuffleAssignmentsResponse response =
        CoordinatorUtils.toGetShuffleAssignmentsResponse(pra, serverNodes);
    logAssignmentResult(appId, shuffleId, pra, serverNodes);

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void heartbeat(
      ShuffleServerHeartBeatRequest request,
      StreamObserver<ShuffleServerHeartBeatResponse> responseObserver) {
    final ServerNode serverNode = toServerNode(request);
    coordinatorServer.getClusterManager().add(serverNode);
    final ShuffleServerHeartBeatResponse response = ShuffleServerHeartBeatResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(StatusCode.SUCCESS)
        .build();
    LOG.debug("Got heartbeat from " + serverNode);
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void checkServiceAvailable(
      Empty request,
      StreamObserver<CheckServiceAvailableResponse> responseObserver) {
    final CheckServiceAvailableResponse response = CheckServiceAvailableResponse
        .newBuilder()
        .setAvailable(coordinatorServer.getClusterManager().getNodesNum() > 0)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void reportClientOperation(
      ReportShuffleClientOpRequest request,
      StreamObserver<ReportShuffleClientOpResponse> responseObserver) {
    final String clientHost = request.getClientHost();
    final int clientPort = request.getClientPort();
    final ShuffleServerId shuffleServer = request.getServer();
    final String operation = request.getOperation();
    LOG.info(clientHost + ":" + clientPort + "->" + operation + "->" + shuffleServer);
    final ReportShuffleClientOpResponse response = ReportShuffleClientOpResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(StatusCode.SUCCESS)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void appHeartbeat(
      AppHeartBeatRequest request,
      StreamObserver<AppHeartBeatResponse> responseObserver) {
    String appId = request.getAppId();
    coordinatorServer.getApplicationManager().refreshAppId(appId);
    LOG.debug("Got heartbeat from application: " + appId);
    AppHeartBeatResponse response = AppHeartBeatResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(StatusCode.SUCCESS)
        .build();

    if (Context.current().isCancelled()) {
      responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
      LOG.warn("Cancelled by client {} for after deadline.", appId);
      return;
    }

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private void logAssignmentResult(String appId, int shuffleId, PartitionRangeAssignment pra,
      List<ServerNode> serverNodesForResult) {
    SortedMap<PartitionRange, List<ServerNode>> assignments = pra.getAssignments();
    if (assignments != null) {
      Set<String> nodeIds = Sets.newHashSet();
      for (Map.Entry<PartitionRange, List<ServerNode>> entry : assignments.entrySet()) {
        for (ServerNode node : entry.getValue()) {
          nodeIds.add(node.getId());
        }
      }
      if (!nodeIds.isEmpty()) {
        Set<String> nodeIdsForResult = Sets.newHashSet();
        for (ServerNode node : serverNodesForResult) {
          nodeIdsForResult.add(node.getId());
        }
        LOG.info("Shuffle Servers of assignment for appId[" + appId + "], shuffleId["
            + shuffleId + "] are " + nodeIds + ", the servers for result are " + nodeIdsForResult);
      }
    }
  }

  private ServerNode toServerNode(ShuffleServerHeartBeatRequest request) {
    return new ServerNode(request.getServerId().getId(),
        request.getServerId().getIp(),
        request.getServerId().getPort(),
        request.getUsedMemory(),
        request.getPreAllocatedMemory(),
        request.getAvailableMemory(),
        request.getEventNumInFlush(),
        Sets.newHashSet(request.getTagsList()));
  }
}
