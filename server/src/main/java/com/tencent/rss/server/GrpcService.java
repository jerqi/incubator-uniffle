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
import com.tencent.rss.proto.RssProtos.GetShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleResultRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleResultResponse;
import com.tencent.rss.proto.RssProtos.PartitionToBlockIds;
import com.tencent.rss.proto.RssProtos.ReportShuffleResultRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleResultResponse;
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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcService extends ShuffleServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcService.class);
  private final ShuffleServer shuffleServer;
  private AtomicLong readDataTime = new AtomicLong(0);

  public GrpcService(ShuffleServer shuffleServer) {
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
    ShuffleServerMetrics.counterTotalRequest.inc();
    ShuffleServerMetrics.counterSendDataRequest.inc();

    SendShuffleDataResponse reply;
    String appId = req.getAppId();
    String shuffleId = String.valueOf(req.getShuffleId());

    StatusCode ret = StatusCode.SUCCESS;
    String responseMessage = "OK";
    if (req.getShuffleDataCount() > 0) {
      try {
        List<ShufflePartitionedData> shufflePartitionedData = toPartitionedData(req);

        long recSize = shufflePartitionedData
            .stream()
            .flatMap(i -> i.getBlockList().stream())
            .map(ShufflePartitionedBlock::getLength)
            .map(i -> Long.valueOf(i)).reduce(0L, Long::sum);
        LOG.debug("Received data {} mb", recSize);

        ShuffleServerMetrics.counterTotalReceivedDataSize.inc(recSize);

        for (ShufflePartitionedData spd : shufflePartitionedData) {
          ShuffleEngine shuffleEngine = shuffleServer
              .getShuffleTaskManager()
              .getShuffleEngine(appId, shuffleId, spd.getPartitionId());

          String shuffleDataInfo = "appId[" + appId + "], shuffleId[" + shuffleId
              + "], partitionId[" + spd.getPartitionId() + "]";

          if (shuffleEngine == null) {
            String errorMsg = "Can't get ShuffleEngine for " + shuffleDataInfo;
            LOG.error(errorMsg);
            ret = StatusCode.NO_REGISTER;
            responseMessage = errorMsg;
            break;
          }
          try {
            long writeTimeout =
                shuffleServer.getShuffleServerConf().get(ShuffleServerConf.SERVER_WRITE_TIMEOUT);
            long start = System.currentTimeMillis();
            do {
              if (System.currentTimeMillis() - start > writeTimeout) {
                String errorMsg = "There is no buffer for "
                    + shuffleDataInfo + " after " + writeTimeout + "ms waiting, statusCode=" + ret;
                LOG.error(errorMsg);
                responseMessage = errorMsg;
                break;
              }
              ret = shuffleEngine.write(spd);
              if (ret == StatusCode.NO_BUFFER) {
                LOG.warn("Buffer is full for writing shuffle data, wait 1s");
                Thread.sleep(1000);
              }
            } while (ret == StatusCode.NO_BUFFER);

            if (ret != StatusCode.SUCCESS && ret != StatusCode.NO_BUFFER) {
              String errorMsg = "Error happened when shuffleEngine.write for "
                  + shuffleDataInfo + ", statusCode=" + ret;
              LOG.error(errorMsg);
              responseMessage = errorMsg;
              break;
            }
          } catch (IOException | IllegalStateException e) {
            String errorMsg = "Error happened when shuffleEngine.write for "
                + shuffleDataInfo + ": " + e.getMessage();
            ret = StatusCode.INTERNAL_ERROR;
            responseMessage = errorMsg;
            LOG.error(errorMsg);
            break;
          }
        }
        reply = SendShuffleDataResponse.newBuilder().setStatus(valueOf(ret)).setRetMsg(responseMessage).build();
      } catch (Exception e) {
        String msg = "Error happened when sendShuffleData ";
        if (!StringUtils.isEmpty(e.getMessage())) {
          msg += e.getMessage();
        }
        reply = SendShuffleDataResponse
            .newBuilder()
            .setStatus(valueOf(StatusCode.INTERNAL_ERROR))
            .setRetMsg(msg)
            .build();
        LOG.error(msg, e);
      }
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

    try {
      status = shuffleServer.getShuffleTaskManager().commitShuffle(appId, shuffleId);
    } catch (Exception e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = e.getMessage();
      LOG.error("Error happened when commit for appId[" + appId + "], shuffleId[" + shuffleId + "]", e);
    }

    reply = ShuffleCommitResponse.newBuilder().setStatus(valueOf(status)).setRetMsg(msg).build();
    responseObserver.onNext(reply);
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
      LOG.error("Report 0 block as shuffle result for " + requestInfo);
    } else {
      try {
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
