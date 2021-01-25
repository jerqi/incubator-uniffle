package com.tencent.rss.client.impl.grpc;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.request.RegisterShuffleRequest;
import com.tencent.rss.client.request.SendCommitRequest;
import com.tencent.rss.client.request.SendShuffleDataRequest;
import com.tencent.rss.client.response.RegisterShuffleResponse;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.SendCommitResponse;
import com.tencent.rss.client.response.SendShuffleDataResponse;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.proto.ShuffleServerGrpc;
import com.tencent.rss.proto.ShuffleServerGrpc.ShuffleServerBlockingStub;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleServerGrpcClient extends GrpcClient implements ShuffleServerClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcClient.class);
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

  public RssProtos.ShuffleRegisterResponse doRegisterShuffle(String appId, int shuffleId, int start, int end) {
    RssProtos.ShuffleRegisterRequest request = RssProtos.ShuffleRegisterRequest.newBuilder().setAppId(appId)
        .setShuffleId(shuffleId).setStart(start).setEnd(end).build();
    return blockingStub.registerShuffle(request);

  }

  public boolean doSendShuffleData(
      String appId, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks) {
    List<ShuffleBlockInfo> shuffleBlockInfos = Lists.newArrayList();
    // prepare rpc request based on shuffleId -> partitionId -> blocks
    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb : shuffleIdToBlocks.entrySet()) {
      List<RssProtos.ShuffleData> shuffleData = Lists.newArrayList();
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        List<RssProtos.ShuffleBlock> shuffleBlocks = Lists.newArrayList();
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          shuffleBlockInfos.add(sbi);
          shuffleBlocks.add(RssProtos.ShuffleBlock.newBuilder().setBlockId(sbi.getBlockId())
              .setCrc(sbi.getCrc())
              .setLength(sbi.getLength())
              .setData(ByteString.copyFrom(sbi.getData()))
              .build());
        }
        shuffleData.add(RssProtos.ShuffleData.newBuilder().setPartitionId(ptb.getKey())
            .addAllBlock(shuffleBlocks)
            .build());
      }
      RssProtos.SendShuffleDataRequest request = RssProtos.SendShuffleDataRequest.newBuilder()
          .setAppId(appId)
          .setShuffleId(stb.getKey())
          .addAllShuffleData(shuffleData)
          .build();
      RssProtos.SendShuffleDataResponse response = blockingStub.sendShuffleData(request);

      if (response.getStatus() != StatusCode.SUCCESS) {
        StringBuilder sb = new StringBuilder();
        for (ShuffleBlockInfo sbi : shuffleBlockInfos) {
          sb.append(sbi.toString()).append("\n");
        }
        String msg = "Can't send shuffle data to " + host + ":" + port
            + " for " + sb.toString()
            + "statusCode=" + response.getStatus()
            + ", errorMsg:" + response.getRetMsg();
        LOG.warn(msg);
        return false;
      }
    }
    return true;
  }

  public RssProtos.ShuffleCommitResponse doSendCommit(String appId, int shuffleId) {
    RssProtos.ShuffleCommitRequest request = RssProtos.ShuffleCommitRequest.newBuilder()
        .setAppId(appId).setShuffleId(shuffleId).build();
    return blockingStub.commitShuffleTask(request);
  }

  @Override
  public RegisterShuffleResponse registerShuffle(RegisterShuffleRequest request) {
    RssProtos.ShuffleRegisterResponse rpcResponse = doRegisterShuffle(
        request.getAppId(),
        request.getShuffleId(),
        request.getStart(),
        request.getEnd());

    RegisterShuffleResponse response;
    StatusCode statusCode = rpcResponse.getStatus();
    switch (statusCode) {
      case SUCCESS:
        response = new RegisterShuffleResponse(ResponseStatusCode.SUCCESS);
        break;
      default:
        String msg = "Can't register shuffle to " + host + ":" + port
            + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId()
            + ", start=" + request.getStart() + ", end=" + request.getEnd() + "], "
            + "errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RuntimeException(msg);
    }
    return response;
  }

  @Override
  public SendShuffleDataResponse sendShuffleData(SendShuffleDataRequest request) {
    boolean sendSuccessfully = doSendShuffleData(request.getAppId(), request.getShuffleIdToBlocks());
    SendShuffleDataResponse response;

    if (sendSuccessfully) {
      response = new SendShuffleDataResponse(ResponseStatusCode.SUCCESS);
    } else {
      response = new SendShuffleDataResponse(ResponseStatusCode.INTERNAL_ERROR);
    }
    return response;
  }

  @Override
  public SendCommitResponse sendCommit(SendCommitRequest request) {
    RssProtos.ShuffleCommitResponse rpcResponse = doSendCommit(request.getAppId(), request.getShuffleId());

    SendCommitResponse response;
    if (rpcResponse.getStatus() != StatusCode.SUCCESS) {
      String msg = "Can't commit shuffle data to " + host + ":" + port
          + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId() + "], "
          + "errorMsg:" + rpcResponse.getRetMsg();
      LOG.error(msg);
      throw new RuntimeException(msg);
    } else {
      response = new SendCommitResponse(ResponseStatusCode.SUCCESS);
    }
    return response;
  }
}
