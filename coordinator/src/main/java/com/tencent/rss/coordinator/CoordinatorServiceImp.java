package com.tencent.rss.coordinator;

import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.RssProtos.ServerRegisterResponse;
import io.grpc.stub.StreamObserver;

/**
 * Implementation class for services defined in protobuf
 */
public class CoordinatorServiceImp extends CoordinatorServerGrpc.CoordinatorServerImplBase {

    private final CoordinatorConf coordinatorConf;

    CoordinatorServiceImp(CoordinatorConf coordinatorConf) {
        this.coordinatorConf = coordinatorConf;
    }

    public void registerShuffleServer(com.tencent.rss.proto.RssProtos.ServerRegisterRequest request,
            StreamObserver<ServerRegisterResponse> responseObserver) {
    }

    public void removeShuffleServer(com.tencent.rss.proto.RssProtos.ServerRemoveRequest request,
            StreamObserver<com.tencent.rss.proto.RssProtos.ServerRemoveResponse> responseObserver) {
    }

    public void getShuffleServerList(com.google.protobuf.Empty request,
            StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse> responseObserver) {
    }

    public void getShuffleServerNum(com.google.protobuf.Empty request,
            StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleServerNumResponse> responseObserver) {
    }

    public void getShuffleAssignments(com.tencent.rss.proto.RssProtos.GetShuffleServerRequest request,
            StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse> responseObserver) {
    }

    public void heartbeat(com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest request,
            StreamObserver<com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse> responseObserver) {
    }

    public void getShuffleDataStorageInfo(com.google.protobuf.Empty request,
            StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleDataStorageInfoResponse>
                    responseObserver) {
    }

    public void checkServiceAvailable(com.google.protobuf.Empty request,
            StreamObserver<com.tencent.rss.proto.RssProtos.CheckServiceAvailableResponse> responseObserver) {
    }

    public void reportClientOperation(com.tencent.rss.proto.RssProtos.ReportShuffleClientOpRequest request,
            StreamObserver<com.tencent.rss.proto.RssProtos.ReportShuffleClientOpResponse> responseObserver) {
    }
}
