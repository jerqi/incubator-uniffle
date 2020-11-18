package com.tencent.rss.common;

import com.google.protobuf.Empty;
import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.CoordinatorServerGrpc.CoordinatorServerBlockingStub;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.CheckServiceAvailableResponse;
import com.tencent.rss.proto.RssProtos.ServerRegisterRequest;
import com.tencent.rss.proto.RssProtos.ServerRegisterResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.proto.RssProtos.StatusCode;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoordinatorGrpcClient {

    private static final Logger logger = LoggerFactory.getLogger(CoordinatorGrpcClient.class);
    private ManagedChannel channel;
    private CoordinatorServerBlockingStub blockingStub;
    private String host;
    private int port;
    private boolean usePlaintext;
    private int maxRetryAttempts;

    public CoordinatorGrpcClient(String host, int port) {
        this(host, port, 3);
    }

    public CoordinatorGrpcClient(String host, int port, int maxRetryAttempts) {
        this(host, port, maxRetryAttempts, true);
    }

    public CoordinatorGrpcClient(String host, int port, int maxRetryAttempts, boolean usePlaintext) {
        this.host = host;
        this.port = port;
        this.maxRetryAttempts = maxRetryAttempts;
        this.usePlaintext = usePlaintext;
        init();
    }

    public CoordinatorGrpcClient(Channel channel) {
        blockingStub = CoordinatorServerGrpc.newBlockingStub(channel);
    }

    public void init() {
        // build channel
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(host, port);

        if (usePlaintext) {
            channelBuilder.usePlaintext();
        }

        if (maxRetryAttempts > 0) {
            channelBuilder.enableRetry().maxRetryAttempts(maxRetryAttempts);
        }

        channel = channelBuilder.build();
        blockingStub = CoordinatorServerGrpc.newBlockingStub(channel);
    }

    public ServerRegisterResponse register(String id, String ip, int port) {
        ShuffleServerId serverId = ShuffleServerId.newBuilder().setId(id).setIp(ip).setPort(port).build();
        ServerRegisterRequest request = ServerRegisterRequest.newBuilder().setServerId(serverId).build();
        ServerRegisterResponse response = blockingStub.registerShuffleServer(request);

        StatusCode status = response.getStatus();
        if (status != StatusCode.SUCCESS) {
            logger.error("Fail to register {}:{} {}", host, port, status);
        }

        return response;
    }

    public ShuffleServerHeartBeatResponse sendHeartBeat(String id, String ip, int port) {
        ShuffleServerId serverId =
                ShuffleServerId.newBuilder().setId(id).setIp(ip).setPort(port).build();
        ShuffleServerHeartBeatRequest request =
                ShuffleServerHeartBeatRequest.newBuilder().setServerId(serverId).build();
        ShuffleServerHeartBeatResponse response = blockingStub.heartbeat(request);

        StatusCode status = response.getStatus();
        if (status != StatusCode.SUCCESS) {
            logger.error("Fail to send heartbeat to {}:{} {}", host, port, status);
        }

        return response;
    }

    public boolean isRssAvailable() {
        CheckServiceAvailableResponse response = blockingStub.checkServiceAvailable(Empty.getDefaultInstance());
        return response.getAvailable();
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
