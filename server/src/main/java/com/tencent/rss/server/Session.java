package com.tencent.rss.server;

import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;


public class Session {
  private String key;
  private ShuffleBuffer buffer;
  private ShuffleDataHandler handler;

  public Session(String key) {
  }

  public String getKey() {
    return key;
  }

  /**
   * Create ShuffleBuffer and ShuffleDataHandler
   *
   * @return
   */
  public boolean init() {
    return true;
  }

  public SendShuffleDataResponse processShuffleData(SendShuffleDataRequest req) {
    SendShuffleDataResponse.Builder replyBuilder = SendShuffleDataResponse.newBuilder();
    replyBuilder.setStatus(StatusCode.SUCCESS);
    return replyBuilder.build();
  }

  public ShuffleCommitResponse processShuffleCommit(ShuffleCommitRequest req) {
    ShuffleCommitResponse.Builder replyBuilder = ShuffleCommitResponse.newBuilder();
    replyBuilder.setStatus(StatusCode.SUCCESS);
    return replyBuilder.build();
  }

}
