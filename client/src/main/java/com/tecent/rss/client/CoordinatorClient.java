package com.tecent.rss.client;

import com.tecent.rss.client.request.GetShuffleAssignmentsRequest;
import com.tecent.rss.client.request.SendHeartBeatRequest;
import com.tecent.rss.client.response.GetShuffleAssignmentsResponse;
import com.tecent.rss.client.response.SendHeartBeatResponse;

public interface CoordinatorClient {

  SendHeartBeatResponse sendHeartBeat(SendHeartBeatRequest request);

  GetShuffleAssignmentsResponse getShuffleAssignments(GetShuffleAssignmentsRequest request);

  void close();
}
