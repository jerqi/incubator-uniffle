package com.tencent.rss.common;

import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleServerGrpcClient extends GrpcClient {

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

  public void registerShuffle(String appId, int shuffleId, int start, int end) {
    ShuffleRegisterRequest request = ShuffleRegisterRequest.newBuilder().setAppId(appId)
        .setShuffleId(shuffleId).setStart(start).setEnd(end).build();
    ShuffleRegisterResponse response = blockingStub.registerShuffle(request);
    if (response.getStatus() != StatusCode.SUCCESS) {
      String msg = "Can't register shuffle to " + host + ":" + port
          + " for [appId=" + appId + ", shuffleId=" + shuffleId
          + ", start=" + start + ", end=" + end + "], "
          + "errorMsg:" + response.getRetMsg();
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }

  public boolean sendShuffleData(String appId, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks) {
    List<ShuffleBlockInfo> shuffleBlockInfos = Lists.newArrayList();
    // prepare rpc request based on shuffleId -> partitionId -> blocks
    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb : shuffleIdToBlocks.entrySet()) {
      List<ShuffleData> shuffleDatas = Lists.newArrayList();
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        List<ShuffleBlock> shuffleBlocks = Lists.newArrayList();
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          shuffleBlockInfos.add(sbi);
          shuffleBlocks.add(ShuffleBlock.newBuilder().setBlockId(sbi.getBlockId())
              .setCrc(sbi.getCrc())
              .setLength(sbi.getLength())
              .setData(ByteString.copyFrom(sbi.getData()))
              .build());
        }
        shuffleDatas.add(ShuffleData.newBuilder().setPartitionId(ptb.getKey())
            .addAllBlock(shuffleBlocks)
            .build());
      }
      SendShuffleDataRequest request = SendShuffleDataRequest.newBuilder()
          .setAppId(appId)
          .setShuffleId(stb.getKey())
          .addAllShuffleData(shuffleDatas)
          .build();
      SendShuffleDataResponse response = blockingStub.sendShuffleData(request);

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

  public void commitShuffleTask(String appId, int shuffleId) {
    ShuffleCommitRequest request = ShuffleCommitRequest.newBuilder()
        .setAppId(appId).setShuffleId(shuffleId).build();
    ShuffleCommitResponse response = blockingStub.commitShuffleTask(request);
    if (response.getStatus() != StatusCode.SUCCESS) {
      String msg = "Can't commit shuffle data to " + host + ":" + port
          + " for [appId=" + appId + ", shuffleId=" + shuffleId + "], "
          + "errorMsg:" + response.getRetMsg();
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }
}
