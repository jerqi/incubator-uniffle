package com.tencent.rss.coordinator;

import com.tencent.rss.proto.CoordinatorServerGrpc;

/**
 * Implementation class for services defined in protobuf
 */
public class CoordinatorServiceImp extends CoordinatorServerGrpc.CoordinatorServerImplBase {
    private final CoordinatorConf coordinatorConf;

    CoordinatorServiceImp(CoordinatorConf coordinatorConf) {
        this.coordinatorConf = coordinatorConf;
    }

    public void registerShuffleServer(com.tencent.rss.proto.RssProtos.ServerRegisterRequest request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.ServerRegisterResponse> responseObserver) {
    }

    public void removeShuffleServer(com.tencent.rss.proto.RssProtos.ServerRemoveRequest request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.ServerRemoveResponse> responseObserver) {
    }

    public void getShuffleServerList(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse> responseObserver) {
    }

    public void getShuffleServerNum(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleServerNumResponse> responseObserver) {
    }

    public void getShuffleServer(com.tencent.rss.proto.RssProtos.GetShuffleServerRequest request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleServerResponse> responseObserver) {
    }

    public void heartbeat(com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse> responseObserver) {
    }

    public void getShuffleDataStorageInfo(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleDataStorageInfoResponse>
        responseObserver) {
    }

    public void checkServiceAvailable(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.CheckServiceAvailableResponse> responseObserver) {
    }

    public void reportClientOperation(com.tencent.rss.proto.RssProtos.ReportShuffleClientOpRequest request,
        io.grpc.stub.StreamObserver<com.tencent.rss.proto.RssProtos.ReportShuffleClientOpResponse> responseObserver) {
    }
}
