package com.tecent.rss.client;

import com.tecent.rss.client.request.RegisterShuffleRequest;
import com.tecent.rss.client.request.SendCommitRequest;
import com.tecent.rss.client.request.SendShuffleDataRequest;
import com.tecent.rss.client.response.RegisterShuffleResponse;
import com.tecent.rss.client.response.SendCommitResponse;
import com.tecent.rss.client.response.SendShuffleDataResponse;

public interface ShuffleServerClient {

  RegisterShuffleResponse registerShuffle(RegisterShuffleRequest request);

  SendShuffleDataResponse sendShuffleData(SendShuffleDataRequest request);

  SendCommitResponse sendCommit(SendCommitRequest request);

  void close();
}
