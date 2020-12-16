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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class RemoteShuffleService extends ShuffleServerImplBase {

  private static final Logger logger = LoggerFactory.getLogger(RemoteShuffleService.class);

  @Override
  public void registerShuffle(ShuffleRegisterRequest req,
                              StreamObserver<ShuffleRegisterResponse> responseObserver) {
    ShuffleRegisterResponse reply;
    String appId = req.getAppId();
    String shuffleId = String.valueOf(req.getShuffleId());
    int start = req.getStart();
    int end = req.getEnd();

    String msg = "";
    StatusCode result = StatusCode.SUCCESS;
    try {
      result = ShuffleTaskManager
        .instance()
        .registerShuffle(appId, shuffleId, start, end);
    } catch (IOException | IllegalStateException e) {
      logger.error("Fail to register shuffle {} {} {} {}", appId, shuffleId, start, end);
      result = StatusCode.INTERNAL_ERROR;
      msg = e.getMessage();
    }

    reply = ShuffleRegisterResponse
      .newBuilder()
      .setStatus(result)
      .setRetMsg(msg)
      .build();

    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void sendShuffleData(SendShuffleDataRequest req,
                              StreamObserver<SendShuffleDataResponse> responseObserver) {
    SendShuffleDataResponse reply;
    String appId = req.getAppId();
    String shuffleId = String.valueOf(req.getShuffleId());

    if (req.getShuffleDataCount() > 0) {
      int partition = req.getShuffleData(0).getPartitionId();
      ShuffleEngine shuffleEngine = ShuffleTaskManager
        .instance()
        .getShuffleEngine(appId, shuffleId, partition);

      StatusCode ret;
      String msg = "";
      if (shuffleEngine == null) {
        ret = StatusCode.NO_REGISTER;
      } else {
        try {
          ret = shuffleEngine.write(toPartitionedData(req));
        } catch (IOException | IllegalStateException e) {
          ret = StatusCode.INTERNAL_ERROR;
          msg = e.getMessage();
          logger.error("Fail to write shuffle data {} {} for {}", appId, shuffleId, msg);
        }
      }
      reply = SendShuffleDataResponse.newBuilder().setStatus(ret).setRetMsg(msg).build();
    } else {
      reply = SendShuffleDataResponse
        .newBuilder()
        .setStatus(StatusCode.INTERNAL_ERROR)
        .setRetMsg("No data in request")
        .build();
    }

    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }

  @Override
  public void commitShuffleTask(ShuffleCommitRequest req,
                                StreamObserver<ShuffleCommitResponse> responseObserver) {
    ShuffleCommitResponse reply;
    String appId = req.getAppId();
    String shuffleId = String.valueOf(req.getShuffleId());

    StatusCode status;
    String msg = "";

    try {
      status = ShuffleTaskManager.instance().commitShuffle(appId, shuffleId);
    } catch (IOException | IllegalStateException e) {
      status = StatusCode.INTERNAL_ERROR;
      msg = e.getMessage();
    }

    reply = ShuffleCommitResponse.newBuilder().setStatus(status).setRetMsg(msg).build();
    responseObserver.onNext(reply);
    responseObserver.onCompleted();
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
