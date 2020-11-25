package com.tencent.rss.common;

import com.google.protobuf.ByteString;
import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.RssProtos.ShuffleData;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterRequest;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.proto.ShuffleServerGrpc;
import com.tencent.rss.proto.ShuffleServerGrpc.ShuffleServerBlockingStub;
import io.grpc.ManagedChannel;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleServerGrpcClient extends GrpcClient {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServerGrpcClient.class);
    private ShuffleServerBlockingStub blockingStub;

    public ShuffleServerGrpcClient(String host, int port) {
        this(host, port, 3);
    }

    public ShuffleServerGrpcClient(String host, int port, int maxRetryAttempts) {
        this(host, port, maxRetryAttempts, true);
    }

    public ShuffleServerGrpcClient(String host, int port, int maxRetryAttempts, boolean usePlaintext) {
        super(host, port, maxRetryAttempts, usePlaintext);
        blockingStub = ShuffleServerGrpc.newBlockingStub(channel);
    }

    public ShuffleServerGrpcClient(ManagedChannel channel) {
        super(channel);
    }

    public void registerShuffle(String appId, int shuffleId, int start, int end) {
        ShuffleRegisterRequest request = ShuffleRegisterRequest.newBuilder().setAppId(appId)
                .setShuffleId(shuffleId).setStart(start).setEnd(end).build();
        ShuffleRegisterResponse response = blockingStub.registerShuffle(request);
        if (response.getStatus() != StatusCode.SUCCESS) {
            throw new RuntimeException("Can't register shuffle to " + host + ":" + port
                    + " for [appId=" + appId + ", shuffleId=" + shuffleId
                    + ", start=" + start + ", end=" + end + "], "
                    + "errorMsg:" + response.getRetMsg());
        }
    }

    public void sendShuffleData(String appId, ShuffleBlockInfo shuffleBlockInfo) {
        if (shuffleBlockInfo == null) {
            return;
        }
        ShuffleBlock shuffleBlock = ShuffleBlock.newBuilder().setBlockId(shuffleBlockInfo.getBlockId())
                .setCrc(shuffleBlockInfo.getCrc())
                .setLength(shuffleBlockInfo.getLength())
                .setData(ByteString.copyFrom(shuffleBlockInfo.getData()))
                .build();
        ShuffleData shuffleData = ShuffleData.newBuilder().setPartitionId(shuffleBlockInfo.getPartitionId())
                .addAllBlock(Arrays.asList(shuffleBlock))
                .build();
        SendShuffleDataRequest request = SendShuffleDataRequest.newBuilder()
                .setAppId(appId)
                .setShuffleId(shuffleBlockInfo.getShuffleId())
                .addAllShuffleData(Arrays.asList(shuffleData))
                .build();
        SendShuffleDataResponse response = blockingStub.sendShuffleData(request);
        if (response.getStatus() != StatusCode.SUCCESS) {
            throw new RuntimeException("Can't send shuffle data to " + host + ":" + port
                    + " for [appId=" + appId + ", shuffleId=" + shuffleBlockInfo.getShuffleId()
                    + ", partitionId=" + shuffleBlockInfo.getPartitionId()
                    + ", blockId=" + shuffleBlockInfo.getBlockId()
                    + ", crc=" + shuffleBlockInfo.getCrc()
                    + ", length=" + shuffleBlockInfo.getLength() + "], "
                    + "errorMsg:" + response.getRetMsg());
        }
    }

    public void commitShuffleTask(String appId, int shuffleId) {
        ShuffleCommitRequest request = ShuffleCommitRequest.newBuilder()
                .setAppId(appId).setShuffleId(shuffleId).build();
        ShuffleCommitResponse response = blockingStub.commitShuffleTask(request);
        if (response.getStatus() != StatusCode.SUCCESS) {
            throw new RuntimeException("Can't commit shuffle data to " + host + ":" + port
                    + " for [appId=" + appId + ", shuffleId=" + shuffleId + "], "
                    + "errorMsg:" + response.getRetMsg());
        }
    }
}
