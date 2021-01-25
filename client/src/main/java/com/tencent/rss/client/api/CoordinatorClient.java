package com.tencent.rss.client.api;

import com.tencent.rss.client.request.GetShuffleAssignmentsRequest;
import com.tencent.rss.client.request.SendHeartBeatRequest;
import com.tencent.rss.client.response.GetShuffleAssignmentsResponse;
import com.tencent.rss.client.response.SendHeartBeatResponse;

public interface CoordinatorClient {

  SendHeartBeatResponse sendHeartBeat(SendHeartBeatRequest request);

  GetShuffleAssignmentsResponse getShuffleAssignments(GetShuffleAssignmentsRequest request);

  void close();
}
