package com.tencent.rss.client.api;

import com.tencent.rss.client.request.RegisterShuffleRequest;
import com.tencent.rss.client.request.SendCommitRequest;
import com.tencent.rss.client.request.SendShuffleDataRequest;
import com.tencent.rss.client.response.RegisterShuffleResponse;
import com.tencent.rss.client.response.SendCommitResponse;
import com.tencent.rss.client.response.SendShuffleDataResponse;

public interface ShuffleServerClient {

  RegisterShuffleResponse registerShuffle(RegisterShuffleRequest request);

  SendShuffleDataResponse sendShuffleData(SendShuffleDataRequest request);

  SendCommitResponse sendCommit(SendCommitRequest request);

  void close();
}
