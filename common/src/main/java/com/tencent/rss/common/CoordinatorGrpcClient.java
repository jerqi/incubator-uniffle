package com.tencent.rss.common;

import com.tencent.rss.proto.CoordinatorServerGrpc;
import com.tencent.rss.proto.CoordinatorServerGrpc.CoordinatorServerBlockingStub;
import com.tencent.rss.proto.RssProtos.ServerRegisterRequest;
import com.tencent.rss.proto.RssProtos.ServerRegisterResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;
import com.tencent.rss.proto.RssProtos.StatusCode;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
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

    public CoordinatorGrpcClient() {
        usePlaintext = true;
        maxRetryAttempts = 3;
        init();
    }

    public CoordinatorGrpcClient(Channel channel) {
        blockingStub = CoordinatorServerGrpc.newBlockingStub(channel);
    }

    public boolean init() {
        // load config
        Map<String, ?> serviceConfig = null;

        // build channel
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(host, port);
        channelBuilder.defaultServiceConfig(serviceConfig);

        if (usePlaintext) {
            channelBuilder.usePlaintext();
        }

        if (maxRetryAttempts > 0) {
            channelBuilder.enableRetry().maxRetryAttempts(maxRetryAttempts);
        }

        channel = channelBuilder.build();
        blockingStub = CoordinatorServerGrpc.newBlockingStub(channel);

        return true;
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

}
