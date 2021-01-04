package com.tencent.rss.server;

import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.RssProtos.ShuffleData;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterRequest;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.proto.ShuffleServerGrpc.ShuffleServerImplBase;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteShuffleService extends ShuffleServerImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleService.class);
  private final ShuffleServer shuffleServer;

  public RemoteShuffleService(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
  }

  @Override
  public void registerShuffle(ShuffleRegisterRequest req,
      StreamObserver<ShuffleRegisterResponse> responseObserver) {
    ShuffleServerMetrics.incTotalRequest();
    ShuffleServerMetrics.incRegisterRequest();

    ShuffleRegisterResponse reply;
    String appId = req.getAppId();
    String shuffleId = String.valueOf(req.getShuffleId());
    int start = req.getStart();
    int end = req.getEnd();

    StatusCode result = shuffleServer
        .getShuffleTaskManager()
        .registerShuffle(appId, shuffleId, start, end);

    reply = ShuffleRegisterResponse
        .newBuilder()
        .setStatus(result)
        .build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();

    ShuffleServerMetrics.decTotalRequest();
    ShuffleServerMetrics.decRegisterRequest();
  }

  @Override
  public void sendShuffleData(SendShuffleDataRequest req,
      StreamObserver<SendShuffleDataResponse> responseObserver) {
    ShuffleServerMetrics.incTotalRequest();
    ShuffleServerMetrics.incSendDataRequest();

    SendShuffleDataResponse reply;
    String appId = req.getAppId();
    String shuffleId = String.valueOf(req.getShuffleId());

    StatusCode ret = StatusCode.SUCCESS;
    String responseMessage = "OK";
    if (req.getShuffleDataCount() > 0) {
      try {
        List<ShufflePartitionedData> shufflePartitionedDatas = toPartitionedData(req);
        for (ShufflePartitionedData spd : shufflePartitionedDatas) {
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
            ret = shuffleEngine.write(spd);
            if (ret != StatusCode.SUCCESS) {
              String errorMsg = "Error happened when shuffleEngine.write for "
                  + shuffleDataInfo + ", statusCode=" + ret;
              LOG.error(errorMsg);
              responseMessage = errorMsg;
              break;
            }
          } catch (IOException ioe) {
            String errorMsg = "Error happened when shuffleEngine.write for "
                + shuffleDataInfo + ": " + ioe.getMessage();
            ret = StatusCode.INTERNAL_ERROR;
            responseMessage = errorMsg;
            LOG.error(errorMsg);
            break;
          }
        }
        reply = SendShuffleDataResponse.newBuilder().setStatus(ret).setRetMsg(responseMessage).build();
      } catch (Exception e) {
        String msg = "Error happened when sendShuffleData ";
        if (!StringUtils.isEmpty(e.getMessage())) {
          msg += e.getMessage();
        }
        reply = SendShuffleDataResponse
            .newBuilder()
            .setStatus(StatusCode.INTERNAL_ERROR)
            .setRetMsg(msg)
            .build();
        LOG.error(msg, e);
      }
    } else {
      reply = SendShuffleDataResponse
          .newBuilder()
          .setStatus(StatusCode.INTERNAL_ERROR)
          .setRetMsg("No data in request")
          .build();
    }

    responseObserver.onNext(reply);
    responseObserver.onCompleted();

    ShuffleServerMetrics.decTotalRequest();
    ShuffleServerMetrics.decSendDataRequest();
  }

  @Override
  public void commitShuffleTask(ShuffleCommitRequest req,
      StreamObserver<ShuffleCommitResponse> responseObserver) {
    ShuffleServerMetrics.incTotalRequest();
    ShuffleServerMetrics.incCommitRequest();

    ShuffleCommitResponse reply;
    String appId = req.getAppId();
    String shuffleId = String.valueOf(req.getShuffleId());

    StatusCode status = StatusCode.SUCCESS;
    String msg = "OK";

    try {
      status = shuffleServer.getShuffleTaskManager().commitShuffle(appId, shuffleId);
    } catch (IOException | IllegalStateException e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = e.getMessage();
    }

    reply = ShuffleCommitResponse.newBuilder().setStatus(status).setRetMsg(msg).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();

    ShuffleServerMetrics.decCommitRequest();
    ShuffleServerMetrics.decCommitRequest();
  }

  private List<ShufflePartitionedData> toPartitionedData(SendShuffleDataRequest req) {
    List<ShufflePartitionedData> ret = new LinkedList<>();

    for (ShuffleData data : req.getShuffleDataList()) {
      ret.add(new ShufflePartitionedData(
          data.getPartitionId(),
          toPartitionedBlock(data.getBlockList())));
    }

    return ret;
  }

  private List<ShufflePartitionedBlock> toPartitionedBlock(List<ShuffleBlock> blocks) {
    List<ShufflePartitionedBlock> ret = new LinkedList<>();

    for (ShuffleBlock block : blocks) {
      ret.add(new ShufflePartitionedBlock(
          block.getLength(),
          block.getCrc(),
          block.getBlockId(),
          block.getData().asReadOnlyByteBuffer()));
    }

    return ret;
  }
}
