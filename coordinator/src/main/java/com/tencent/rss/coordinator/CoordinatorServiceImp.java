package com.tencent.rss.coordinator;

import com.tencent.rss.coordinator.metadata.ShuffleServerInfo;
import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.RssProtos.CheckServiceAvailableResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleDataStorageInfoResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleServerNumResponse;
import com.tencent.rss.proto.RssProtos.ReportShuffleClientOpResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.proto.RssProtos.StatusCode;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation class for services defined in protobuf
 */
public class CoordinatorServiceImp extends CoordinatorServerGrpc.CoordinatorServerImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorServiceImp.class);

    private final CoordinatorConf coordinatorConf;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;

    private final Map<ShuffleServerId, ShuffleServerInfo> shuffleServerInfoMap;

    CoordinatorServiceImp(CoordinatorConf coordinatorConf) {
        this.coordinatorConf = coordinatorConf;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        readLock = lock.readLock();
        writeLock = lock.writeLock();
        shuffleServerInfoMap = new HashMap<>();
    }

    private <T> T withReadLock(Supplier<T> func) {
        readLock.lock();
        try {
            return func.get();
        } finally {
            readLock.unlock();
        }
    }

    public void getShuffleServerList(com.google.protobuf.Empty request,
            StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleServerListResponse> responseObserver) {
        final GetShuffleServerListResponse response = GetShuffleServerListResponse
                .newBuilder()
                .addAllServers(shuffleServerInfoMap.keySet())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void getShuffleServerNum(com.google.protobuf.Empty request,
            StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleServerNumResponse> responseObserver) {
        final int num = withReadLock(shuffleServerInfoMap::size);
        final GetShuffleServerNumResponse response = GetShuffleServerNumResponse
                .newBuilder()
                .setNum(num)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void getShuffleAssignments(com.tencent.rss.proto.RssProtos.GetShuffleServerRequest request,
            StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse> responseObserver) {
        final String appId = request.getApplicationId();
        final int shuffleId = request.getShuffleId();
        final int partitionNum = request.getPartitionNum();
        final int partitionPerServer = request.getPartitionPerServer();
        final int replica = coordinatorConf.getShuffleServerDataReplica();

        final GetShuffleAssignmentsResponse response = null;
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void heartbeat(com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest request,
            StreamObserver<com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse> responseObserver) {
        final ShuffleServerId shuffleServerId = request.getServerId();
        writeLock.lock();
        try {
            shuffleServerInfoMap.put(shuffleServerId, ShuffleServerInfo.build(shuffleServerId));
        } finally {
            writeLock.unlock();
        }
        final ShuffleServerHeartBeatResponse response = ShuffleServerHeartBeatResponse
                .newBuilder()
                .setRetMsg("")
                .setStatus(StatusCode.SUCCESS)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void getShuffleDataStorageInfo(com.google.protobuf.Empty request,
            StreamObserver<com.tencent.rss.proto.RssProtos.GetShuffleDataStorageInfoResponse>
                    responseObserver) {
        final String storage = coordinatorConf.getDataStorage();
        final String storagePath = coordinatorConf.getDataStoragePath();
        final String storagePattern = coordinatorConf.getDataStoragePattern();
        final GetShuffleDataStorageInfoResponse response = GetShuffleDataStorageInfoResponse
                .newBuilder()
                .setStorage(storage)
                .setStoragePath(storagePath)
                .setStoragePattern(storagePattern)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void checkServiceAvailable(com.google.protobuf.Empty request,
            StreamObserver<com.tencent.rss.proto.RssProtos.CheckServiceAvailableResponse> responseObserver) {
        final CheckServiceAvailableResponse response = CheckServiceAvailableResponse
                .newBuilder()
                .setAvailable(!shuffleServerInfoMap.isEmpty())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void reportClientOperation(com.tencent.rss.proto.RssProtos.ReportShuffleClientOpRequest request,
            StreamObserver<com.tencent.rss.proto.RssProtos.ReportShuffleClientOpResponse> responseObserver) {
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
