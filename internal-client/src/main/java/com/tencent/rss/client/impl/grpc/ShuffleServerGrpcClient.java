package com.tencent.rss.client.impl.grpc;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssFinishShuffleResponse;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssRegisterShuffleResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.client.response.RssSendCommitResponse;
import com.tencent.rss.client.response.RssSendShuffleDataResponse;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.FinishShuffleRequest;
import com.tencent.rss.proto.RssProtos.FinishShuffleResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleResultRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleResultResponse;
import com.tencent.rss.proto.RssProtos.PartitionToBlockIds;
import com.tencent.rss.proto.RssProtos.ReportShuffleResultRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleResultResponse;
import com.tencent.rss.proto.RssProtos.RequireBufferRequest;
import com.tencent.rss.proto.RssProtos.RequireBufferResponse;
import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.RssProtos.ShuffleData;
import com.tencent.rss.proto.RssProtos.ShuffleDataBlockSegment;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.proto.ShuffleServerGrpc;
import com.tencent.rss.proto.ShuffleServerGrpc.ShuffleServerBlockingStub;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleServerGrpcClient extends GrpcClient implements ShuffleServerClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcClient.class);
  private static final long FAILED_REQUIRE_ID = -1;
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

  public ShuffleCommitResponse doSendCommit(String appId, int shuffleId) {
    ShuffleCommitRequest request = ShuffleCommitRequest.newBuilder()
        .setAppId(appId).setShuffleId(shuffleId).build();
    return blockingStub.commitShuffleTask(request);
  }

  public long requirePreAllocation(int requireSize, int retryMax, long retryIntervalMax) {
    RequireBufferRequest rpcRequest = RequireBufferRequest.newBuilder().setRequireSize(requireSize).build();
    RequireBufferResponse rpcResponse = blockingStub.requireBuffer(rpcRequest);
    int retry = 0;
    long result = FAILED_REQUIRE_ID;
    Random random = new Random();
    final int backOffBase = 2000;
    while (rpcResponse.getStatus() == StatusCode.NO_BUFFER) {
      LOG.info("Can't require " + requireSize + " bytes from " + host + ":" + port + ", sleep and try["
          + retry + "] again");
      if (retry >= retryMax) {
        LOG.warn("ShuffleServer " + host + ":" + port + " is full and can't send shuffle"
            + " data successfully after retry " + retryMax + " times");
        return result;
      }
      try {
        long backoffTime =
            Math.min(retryIntervalMax, backOffBase * (1 << Math.min(retry, 16)) + random.nextInt(backOffBase));
        Thread.sleep(backoffTime);
      } catch (Exception e) {
        LOG.warn("Exception happened when require pre allocation", e);
      }
      rpcResponse = blockingStub.requireBuffer(rpcRequest);
      retry++;
    }
    if (rpcResponse.getStatus() == StatusCode.SUCCESS) {
      result = rpcResponse.getRequireBufferId();
    }
    return result;
  }

  @Override
  public RssRegisterShuffleResponse registerShuffle(RssRegisterShuffleRequest request) {
    ShuffleRegisterResponse rpcResponse = doRegisterShuffle(
        request.getAppId(),
        request.getShuffleId(),
        request.getStart(),
        request.getEnd());

    RssRegisterShuffleResponse response;
    StatusCode statusCode = rpcResponse.getStatus();
    switch (statusCode) {
      case SUCCESS:
        response = new RssRegisterShuffleResponse(ResponseStatusCode.SUCCESS);
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
  public RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request) {
    String appId = request.getAppId();
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = request.getShuffleIdToBlocks();

    List<ShuffleBlockInfo> shuffleBlockInfos = Lists.newArrayList();
    boolean isSuccessful = true;

    // prepare rpc request based on shuffleId -> partitionId -> blocks
    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb : shuffleIdToBlocks.entrySet()) {
      List<ShuffleData> shuffleData = Lists.newArrayList();
      int size = 0;
      int blockNum = 0;
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        List<ShuffleBlock> shuffleBlocks = Lists.newArrayList();
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          shuffleBlockInfos.add(sbi);
          shuffleBlocks.add(ShuffleBlock.newBuilder().setBlockId(sbi.getBlockId())
              .setCrc(sbi.getCrc())
              .setLength(sbi.getLength())
              .setUncompressLength(sbi.getUncompressLength())
              .setData(ByteString.copyFrom(sbi.getData()))
              .build());
          size += sbi.getLength();
          blockNum++;
        }
        shuffleData.add(ShuffleData.newBuilder().setPartitionId(ptb.getKey())
            .addAllBlock(shuffleBlocks)
            .build());
      }

      long requireId = requirePreAllocation(size, request.getRetryMax(), request.getRetryIntervalMax());
      if (requireId != FAILED_REQUIRE_ID) {
        SendShuffleDataRequest rpcRequest = SendShuffleDataRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(stb.getKey())
            .setRequireBufferId(requireId)
            .addAllShuffleData(shuffleData)
            .build();
        long start = System.currentTimeMillis();
        SendShuffleDataResponse response = blockingStub.sendShuffleData(rpcRequest);
        LOG.info("Do sendShuffleData rpc cost:" + (System.currentTimeMillis() - start)
            + " ms for " + size + " bytes with " + blockNum + " blocks");

        if (response.getStatus() != StatusCode.SUCCESS) {
          String msg = "Can't send shuffle data with " + shuffleBlockInfos.size()
              + " blocks to " + host + ":" + port
              + ", statusCode=" + response.getStatus()
              + ", errorMsg:" + response.getRetMsg();
          LOG.warn(msg);
          isSuccessful = false;
          break;
        }
      } else {
        isSuccessful = false;
        break;
      }
    }

    RssSendShuffleDataResponse response;
    if (isSuccessful) {
      response = new RssSendShuffleDataResponse(ResponseStatusCode.SUCCESS);
    } else {
      response = new RssSendShuffleDataResponse(ResponseStatusCode.INTERNAL_ERROR);
    }
    return response;
  }

  @Override
  public RssSendCommitResponse sendCommit(RssSendCommitRequest request) {
    ShuffleCommitResponse rpcResponse = doSendCommit(request.getAppId(), request.getShuffleId());

    RssSendCommitResponse response;
    if (rpcResponse.getStatus() != StatusCode.SUCCESS) {
      String msg = "Can't commit shuffle data to " + host + ":" + port
          + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId() + "], "
          + "errorMsg:" + rpcResponse.getRetMsg();
      LOG.error(msg);
      throw new RuntimeException(msg);
    } else {
      response = new RssSendCommitResponse(ResponseStatusCode.SUCCESS);
      response.setCommitCount(rpcResponse.getCommitCount());
    }
    return response;
  }

  @Override
  public RssFinishShuffleResponse finishShuffle(RssFinishShuffleRequest request) {
    FinishShuffleRequest rpcRequest = FinishShuffleRequest.newBuilder()
        .setAppId(request.getAppId()).setShuffleId(request.getShuffleId()).build();
    FinishShuffleResponse rpcResponse = blockingStub.finishShuffle(rpcRequest);

    RssFinishShuffleResponse response;
    if (rpcResponse.getStatus() != StatusCode.SUCCESS) {
      String msg = "Can't finish shuffle process to " + host + ":" + port
          + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId() + "], "
          + "errorMsg:" + rpcResponse.getRetMsg();
      LOG.error(msg);
      throw new RuntimeException(msg);
    } else {
      response = new RssFinishShuffleResponse(ResponseStatusCode.SUCCESS);
    }
    return response;
  }

  @Override
  public RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request) {
    List<PartitionToBlockIds> partitionToBlockIds = Lists.newArrayList();
    for (Map.Entry<Integer, List<Long>> entry : request.getPartitionToBlockIds().entrySet()) {
      List<Long> blockIds = entry.getValue();
      if (blockIds != null && !blockIds.isEmpty()) {
        partitionToBlockIds.add(PartitionToBlockIds.newBuilder()
            .setPartitionId(entry.getKey())
            .addAllBlockIds(entry.getValue())
            .build());
      }
    }

    ReportShuffleResultRequest recRequest = ReportShuffleResultRequest.newBuilder()
        .setAppId(request.getAppId())
        .setShuffleId(request.getShuffleId())
        .addAllPartitionToBlockIds(partitionToBlockIds)
        .build();
    ReportShuffleResultResponse rpcResponse = blockingStub.reportShuffleResult(recRequest);

    StatusCode statusCode = rpcResponse.getStatus();
    RssReportShuffleResultResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssReportShuffleResultResponse(ResponseStatusCode.SUCCESS);
        break;
      default:
        String msg = "Can't report shuffle result to " + host + ":" + port
            + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId()
            + ", errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RuntimeException(msg);
    }

    return response;
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request) {
    GetShuffleResultRequest rpcRequest = GetShuffleResultRequest
        .newBuilder()
        .setAppId(request.getAppId())
        .setShuffleId(request.getShuffleId())
        .setPartitionId(request.getPartitionId())
        .build();
    GetShuffleResultResponse rpcResponse = blockingStub.getShuffleResult(rpcRequest);
    StatusCode statusCode = rpcResponse.getStatus();

    RssGetShuffleResultResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssGetShuffleResultResponse(ResponseStatusCode.SUCCESS);
        response.setBlockIds(rpcResponse.getBlockIdsList());
        break;
      default:
        String msg = "Can't get shuffle result from " + host + ":" + port
            + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId()
            + ", errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RuntimeException(msg);
    }

    return response;
  }

  @Override
  public RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request) {
    GetShuffleDataRequest rpcRequest = GetShuffleDataRequest
        .newBuilder()
        .setAppId(request.getAppId())
        .setShuffleId(request.getShuffleId())
        .setPartitionId(request.getPartitionId())
        .setPartitionNumPerRange(request.getPartitionNumPerRange())
        .setPartitionNum(request.getPartitionNum())
        .setReadBufferSize(request.getReadBufferSize())
        .addAllBlockIds(request.getBlockIds())
        .build();
    long start = System.currentTimeMillis();
    GetShuffleDataResponse rpcResponse = blockingStub.getShuffleData(rpcRequest);
    LOG.info("GetShuffleData for appId[" + request.getAppId() + "], shuffleId["
        + request.getShuffleId() + "], partitionId[" + request.getPartitionId() + "] cost "
        + (System.currentTimeMillis() - start) + " ms");
    StatusCode statusCode = rpcResponse.getStatus();

    RssGetShuffleDataResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssGetShuffleDataResponse(ResponseStatusCode.SUCCESS);
        ShuffleDataResult sdr = new ShuffleDataResult(
            rpcResponse.getData().toByteArray(),
            toBufferSegments(rpcResponse.getBlockSegmentsList()));
        response.setShuffleDataResult(sdr);
        break;
      default:
        String msg = "Can't get shuffle data from " + host + ":" + port
            + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId()
            + ", errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RuntimeException(msg);
    }
    return response;
  }

  @Override
  public String getClientInfo() {
    return "ShuffleServerGrpcClient for host[" + host + "], port[" + port + "]";
  }

  private List<BufferSegment> toBufferSegments(List<ShuffleDataBlockSegment> blockSegments) {
    List<BufferSegment> ret = Lists.newArrayList();
    for (ShuffleDataBlockSegment segment : blockSegments) {
      ret.add(new BufferSegment(segment.getBlockId(), segment.getOffset(),
          segment.getLength(), segment.getUncompressLength(), segment.getCrc()));
    }
    return ret;
  }
}
