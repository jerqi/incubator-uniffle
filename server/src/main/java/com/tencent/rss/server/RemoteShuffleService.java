package com.tencent.rss.server;

import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterRequest;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.proto.ShuffleServerGrpc;
import io.grpc.stub.StreamObserver;

public class RemoteShuffleService extends ShuffleServerGrpc.ShuffleServerImplBase {

    @Override
    public void registerShuffle(ShuffleRegisterRequest req,
            StreamObserver<ShuffleRegisterResponse> responseObserver) {
        ShuffleRegisterResponse reply;
        String appId = req.getAppId();
        String shuffleId = String.valueOf(req.getShuffleId());
        int start = req.getStart();
        int end = req.getEnd();

        StatusCode result = ShuffleTaskManager
                .instance()
                .registerShuffle(appId, shuffleId, start, end);

        reply = ShuffleRegisterResponse
                .newBuilder()
                .setStatus(result)
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
            if (shuffleEngine == null) {
                ret = StatusCode.NO_REGISTER;
            } else {
                ret = shuffleEngine.write(req.getShuffleDataList());
            }
            reply = SendShuffleDataResponse.newBuilder().setStatus(ret).build();
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
        StatusCode status = ShuffleTaskManager.instance().commitShuffle(appId, shuffleId);

        reply = ShuffleCommitResponse.newBuilder().setStatus(status).build();
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}
