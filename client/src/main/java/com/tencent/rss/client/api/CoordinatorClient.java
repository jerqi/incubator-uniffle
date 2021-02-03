package com.tencent.rss.client.api;

import com.tencent.rss.client.request.RssGetShuffleAssignmentsRequest;
import com.tencent.rss.client.request.RssSendHeartBeatRequest;
import com.tencent.rss.client.response.RssGetShuffleAssignmentsResponse;
import com.tencent.rss.client.response.RssSendHeartBeatResponse;

public interface CoordinatorClient {

  RssSendHeartBeatResponse sendHeartBeat(RssSendHeartBeatRequest request);

  RssGetShuffleAssignmentsResponse getShuffleAssignments(RssGetShuffleAssignmentsRequest request);

  void close();
}
