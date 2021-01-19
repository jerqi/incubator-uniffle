package com.tencent.rss.coordinator;

import com.google.protobuf.Empty;
import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.RssProtos.CheckServiceAvailableResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleDataStorageInfoResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerNumResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleClientOpRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleClientOpResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.proto.RssProtos.StatusCode;
import io.grpc.stub.StreamObserver;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation class for services defined in protobuf
 */
public class CoordinatorServiceImpl extends CoordinatorServerGrpc.CoordinatorServerImplBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorServiceImpl.class);

  private final CoordinatorServer coordinatorServer;

  CoordinatorServiceImpl(CoordinatorServer coordinatorServer) {
    this.coordinatorServer = coordinatorServer;
  }

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

  public void getShuffleAssignments(
      GetShuffleServerRequest request,
      StreamObserver<GetShuffleAssignmentsResponse> responseObserver) {
    final int partitionNum = request.getPartitionNum();
    final int partitionPerServer = request.getPartitionPerServer();
    final int replica = coordinatorServer.getCoordinatorConf().getShuffleServerDataReplica();

    final PartitionRangeAssignment pra =
        coordinatorServer.getAssignmentStrategy().assign(partitionNum, partitionPerServer, replica);
    final GetShuffleAssignmentsResponse response = pra.convertToGrpcProto();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  public void heartbeat(
      ShuffleServerHeartBeatRequest request,
      StreamObserver<ShuffleServerHeartBeatResponse> responseObserver) {
    final ServerNode serverNode = ServerNode.valueOf(request);
    coordinatorServer.getClusterManager().add(serverNode);
    final ShuffleServerHeartBeatResponse response = ShuffleServerHeartBeatResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(StatusCode.SUCCESS)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  public void getShuffleDataStorageInfo(
      Empty request,
      StreamObserver<GetShuffleDataStorageInfoResponse> responseObserver) {
    final String storage = coordinatorServer.getCoordinatorConf().getDataStorage();
    final String storagePath = coordinatorServer.getCoordinatorConf().getDataStoragePath();
    final String storagePattern = coordinatorServer.getCoordinatorConf().getDataStoragePattern();
    final GetShuffleDataStorageInfoResponse response = GetShuffleDataStorageInfoResponse
        .newBuilder()
        .setStorage(storage)
        .setStoragePath(storagePath)
        .setStoragePattern(storagePattern)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

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

  public void reportClientOperation(
      ReportShuffleClientOpRequest request,
      StreamObserver<ReportShuffleClientOpResponse> responseObserver) {
    final String clientHost = request.getClientHost();
    final int clientPort = request.getClientPort();
    final ShuffleServerId shuffleServer = request.getServer();
    final String operation = request.getOperation();
    LOGGER.info(clientHost + ":" + clientPort + "->" + operation + "->" + shuffleServer);
    final ReportShuffleClientOpResponse response = ReportShuffleClientOpResponse
        .newBuilder()
        .setRetMsg("")
        .setStatus(StatusCode.SUCCESS)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

}
