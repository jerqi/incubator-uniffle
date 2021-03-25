package com.tencent.rss.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.config.RssBaseConf;
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
import com.tencent.rss.proto.RssProtos.ShuffleRegisterRequest;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterResponse;
import com.tencent.rss.proto.ShuffleServerGrpc.ShuffleServerImplBase;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleServerGrpcService extends ShuffleServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcService.class);
  private final ShuffleServer shuffleServer;
  private AtomicLong readDataTime = new AtomicLong(0);

  public ShuffleServerGrpcService(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
  }

  public static RssProtos.StatusCode valueOf(StatusCode code) {
    switch (code) {
      case SUCCESS:
        return RssProtos.StatusCode.SUCCESS;
      case DOUBLE_REGISTER:
        return RssProtos.StatusCode.DOUBLE_REGISTER;
      case NO_BUFFER:
        return RssProtos.StatusCode.NO_BUFFER;
      case INVALID_STORAGE:
        return RssProtos.StatusCode.INVALID_STORAGE;
      case NO_REGISTER:
        return RssProtos.StatusCode.NO_REGISTER;
      case NO_PARTITION:
        return RssProtos.StatusCode.NO_PARTITION;
      case TIMEOUT:
        return RssProtos.StatusCode.TIMEOUT;
      default:
        return RssProtos.StatusCode.INTERNAL_ERROR;
    }
  }

  @Override
  public void registerShuffle(ShuffleRegisterRequest req,
      StreamObserver<ShuffleRegisterResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterRegisterRequest.inc();

    ShuffleRegisterResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    int start = req.getStart();
    int end = req.getEnd();

    StatusCode result = shuffleServer
        .getShuffleTaskManager()
        .registerShuffle(appId, shuffleId, start, end);

    reply = ShuffleRegisterResponse
        .newBuilder()
        .setStatus(valueOf(result))
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void sendShuffleData(SendShuffleDataRequest req,
      StreamObserver<SendShuffleDataResponse> responseObserver) {
    long s = System.currentTimeMillis();
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterSendDataRequest.inc();

    SendShuffleDataResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    long requireBufferId = req.getRequireBufferId();
    int requireSize = shuffleServer
        .getShuffleTaskManager().getRequireBufferSize(requireBufferId);

    StatusCode ret = StatusCode.SUCCESS;
    String responseMessage = "OK";
    if (req.getShuffleDataCount() > 0) {
      ShuffleServerMetrics.counterTotalReceivedDataSize.inc(requireSize);
      boolean isPreAllocated = shuffleServer.getShuffleTaskManager().isPreAllocated(requireBufferId);
      if (!isPreAllocated) {
        LOG.warn("Can't find requireBufferId[" + requireBufferId + "] for appId[" + appId
            + "], shuffleId[" + shuffleId + "]");
      }
      final long start = System.currentTimeMillis();
      List<ShufflePartitionedData> shufflePartitionedData = toPartitionedData(req);
      for (ShufflePartitionedData spd : shufflePartitionedData) {
        String shuffleDataInfo = "appId[" + appId + "], shuffleId[" + shuffleId
            + "], partitionId[" + spd.getPartitionId() + "]";
        try {
          ret = shuffleServer
              .getShuffleTaskManager()
              .cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
          if (ret != StatusCode.SUCCESS) {
            String errorMsg = "Error happened when shuffleEngine.write for "
                + shuffleDataInfo + ", statusCode=" + ret;
            LOG.error(errorMsg);
            responseMessage = errorMsg;
            break;
          } else {
            shuffleServer.getShuffleTaskManager().updateCachedBlockCount(
                appId, shuffleId, spd.getBlockList().size());
          }
        } catch (Exception e) {
          String errorMsg = "Error happened when shuffleEngine.write for "
              + shuffleDataInfo + ": " + e.getMessage();
          ret = StatusCode.INTERNAL_ERROR;
          responseMessage = errorMsg;
          LOG.error(errorMsg);
          break;
        }
      }
      shuffleServer
          .getShuffleTaskManager().removeRequireBufferId(requireBufferId);
      reply = SendShuffleDataResponse.newBuilder().setStatus(valueOf(ret)).setRetMsg(responseMessage).build();
      LOG.debug("Cache Shuffle Data for appId[" + appId + "], shuffleId[" + shuffleId
          + "], cost " + (System.currentTimeMillis() - start)
          + " ms with " + shufflePartitionedData.size() + " blocks and " + requireSize
          + " bytes, the current block num is about "
          + shuffleServer.getShuffleTaskManager().getCachedBlockCount(appId, shuffleId));
    } else {
      reply = SendShuffleDataResponse
          .newBuilder()
          .setStatus(valueOf(StatusCode.INTERNAL_ERROR))
          .setRetMsg("No data in request")
          .build();
    }

    responseObserver.onNext(reply);
    responseObserver.onCompleted();

  }

  @Override
  public void commitShuffleTask(ShuffleCommitRequest req,
      StreamObserver<ShuffleCommitResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterCommitRequest.inc();

    ShuffleCommitResponse reply;
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();

    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    int commitCount = 0;

    try {
      commitCount = shuffleServer.getShuffleTaskManager().updateAndGetCommitCount(appId, shuffleId);
      LOG.info("Got commitShuffleTask request for appId[" + appId + "], shuffleId["
          + shuffleId + "], currentCommitted[" + commitCount + "]");
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = "Error happened when commit for appId[" + appId + "], shuffleId[" + shuffleId + "]";
      LOG.error(msg, e);
    }

    reply = ShuffleCommitResponse
        .newBuilder()
        .setCommitCount(commitCount)
        .setStatus(valueOf(status))
        .setRetMsg(msg)
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void finishShuffle(FinishShuffleRequest req,
      StreamObserver<FinishShuffleResponse> responseObserver) {
    String appId = req.getAppId();
    int shuffleId = req.getShuffleId();
    StatusCode status;
    String msg = "OK";
    String errorMsg = "Fail to finish shuffle for appId["
        + appId + "], shuffleId[" + shuffleId + "], data may be lost";
    try {
      LOG.info("Got finishShuffle request for appId[" + appId + "], shuffleId[" + shuffleId + "]");
      status = shuffleServer.getShuffleTaskManager().commitShuffle(appId, shuffleId);
      if (status != StatusCode.SUCCESS) {
        status = StatusCode.INTERNAL_ERROR;
        msg = errorMsg;
      }
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = errorMsg;
      LOG.error(errorMsg, e);
    }

    FinishShuffleResponse response =
        FinishShuffleResponse
            .newBuilder()
            .setStatus(valueOf(status))
            .setRetMsg(msg).build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void requireBuffer(RequireBufferRequest request,
      StreamObserver<RequireBufferResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    long requireBufferId = shuffleServer.getShuffleTaskManager().requireBuffer(request.getRequireSize());
    StatusCode status = StatusCode.SUCCESS;
    if (requireBufferId == -1) {
      status = StatusCode.NO_BUFFER;
    }
    RequireBufferResponse response =
        RequireBufferResponse
            .newBuilder()
            .setStatus(valueOf(status))
            .setRequireBufferId(requireBufferId)
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void reportShuffleResult(ReportShuffleResultRequest request,
      StreamObserver<ReportShuffleResultResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();
    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    Map<Integer, List<Long>> partitionToBlockIds = toPartionBlocksMap(request.getPartitionToBlockIdsList());
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    ReportShuffleResultResponse reply;
    String requestInfo = "appId[" + appId + "], shuffleId[" + shuffleId + "]";

    if (partitionToBlockIds.isEmpty()) {
      LOG.warn("Report 0 block as shuffle result for " + requestInfo);
    } else {
      try {
        LOG.info("Report " + partitionToBlockIds.size() + " blocks as shuffle result for the task of " + requestInfo);
        shuffleServer.getShuffleTaskManager().addFinishedBlockIds(appId, shuffleId, partitionToBlockIds);
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = e.getMessage();
        LOG.error("Error happened when report shuffle result for " + requestInfo, e);
      }
    }

    reply = ReportShuffleResultResponse.newBuilder().setStatus(valueOf(status)).setRetMsg(msg).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleResult(GetShuffleResultRequest request,
      StreamObserver<GetShuffleResultResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();

    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetShuffleResultResponse reply;
    List<Long> blockIds = Lists.newArrayList();
    String requestInfo = "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId[" + partitionId + "]";

    try {
      blockIds = shuffleServer.getShuffleTaskManager().getFinishedBlockIds(appId, shuffleId, partitionId);
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = e.getMessage();
      LOG.error("Error happened when report shuffle result for " + requestInfo, e);
    }
    reply = GetShuffleResultResponse.newBuilder()
        .setStatus(valueOf(status))
        .setRetMsg(msg)
        .addAllBlockIds(blockIds).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void getShuffleData(GetShuffleDataRequest request,
      StreamObserver<GetShuffleDataResponse> responseObserver) {
    ShuffleServerMetrics.counterTotalRequest.inc();

    String appId = request.getAppId();
    int shuffleId = request.getShuffleId();
    int partitionId = request.getPartitionId();
    int partitionsPerServer = request.getPartitionsPerServer();
    int partitionNum = request.getPartitionNum();
    int readBufferSize = request.getReadBufferSize();
    String storageType = shuffleServer.getShuffleServerConf().get(RssBaseConf.DATA_STORAGE_TYPE);
    Set<Long> blockIds = Sets.newHashSet(request.getBlockIdsList());
    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";
    GetShuffleDataResponse reply;
    ShuffleDataResult sdr;
    String requestInfo = "appId[" + appId + "], shuffleId[" + shuffleId + "], partitionId["
        + partitionId + "]";

    if (shuffleServer.getShuffleBufferManager().requireMemoryWithRetry(readBufferSize)) {
      try {
        long start = System.currentTimeMillis();
        sdr = shuffleServer.getShuffleTaskManager().getShuffleData(appId, shuffleId, partitionId,
            partitionsPerServer, partitionNum, readBufferSize, storageType, blockIds);
        readDataTime.addAndGet(System.currentTimeMillis() - start);
        LOG.debug("Rpc server[getShuffleData] cost " + (System.currentTimeMillis() - start)
            + " ms for " + requestInfo);
        reply = GetShuffleDataResponse.newBuilder()
            .setStatus(valueOf(status))
            .setRetMsg(msg)
            .setData(ByteString.copyFrom(sdr.getData()))
            .addAllBlockSegments(toBlockSegments(sdr.getBufferSegments()))
            .build();
      } catch (Exception e) {
        status = StatusCode.INTERNAL_ERROR;
        msg = e.getMessage();
        LOG.error("Error happened when get shuffle data for " + requestInfo, e);
        reply = GetShuffleDataResponse.newBuilder()
            .setStatus(valueOf(status))
            .setRetMsg(msg)
            .build();
      } finally {
        shuffleServer.getShuffleBufferManager().releaseMemory(readBufferSize, false);
      }
    } else {
      status = StatusCode.INTERNAL_ERROR;
      msg = "Can't require memory to get shuffle data";
      LOG.error(msg + " for " + requestInfo);
      reply = GetShuffleDataResponse.newBuilder()
          .setStatus(valueOf(status))
          .setRetMsg(msg)
          .build();
    }

    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  private List<ShuffleDataBlockSegment> toBlockSegments(List<BufferSegment> bufferSegments) {
    List<ShuffleDataBlockSegment> ret = Lists.newArrayList();

    for (BufferSegment segment : bufferSegments) {
      ret.add(ShuffleDataBlockSegment.newBuilder()
          .setBlockId(segment.getBlockId())
          .setOffset(segment.getOffset())
          .setLength(segment.getLength())
          .setUncompressLength(segment.getUncompressLength())
          .setCrc(segment.getCrc())
          .build());
    }

    return ret;
  }

  private List<ShufflePartitionedData> toPartitionedData(SendShuffleDataRequest req) {
    List<ShufflePartitionedData> ret = Lists.newArrayList();

    for (ShuffleData data : req.getShuffleDataList()) {
      ret.add(new ShufflePartitionedData(
          data.getPartitionId(),
          toPartitionedBlock(data.getBlockList())));
    }

    return ret;
  }

  private List<ShufflePartitionedBlock> toPartitionedBlock(List<ShuffleBlock> blocks) {
    List<ShufflePartitionedBlock> ret = Lists.newArrayList();

    for (ShuffleBlock block : blocks) {
      ret.add(new ShufflePartitionedBlock(
          block.getLength(),
          block.getUncompressLength(),
          block.getCrc(),
          block.getBlockId(),
          block.getData().asReadOnlyByteBuffer()));
    }

    return ret;
  }

  private Map<Integer, List<Long>> toPartionBlocksMap(List<PartitionToBlockIds> partitionToBlockIds) {
    Map<Integer, List<Long>> result = Maps.newHashMap();
    for (PartitionToBlockIds ptb : partitionToBlockIds) {
      List<Long> blockIds = ptb.getBlockIdsList();
      if (blockIds != null && !blockIds.isEmpty()) {
        result.put(ptb.getPartitionId(), blockIds);
      }
    }
    return result;
  }
}
