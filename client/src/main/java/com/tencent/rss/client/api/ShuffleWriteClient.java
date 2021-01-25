package com.tencent.rss.client.api;

import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.client.response.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Set;

public interface ShuffleWriteClient {

  SendShuffleDataResult sendShuffleData(String appId, List<ShuffleBlockInfo> shuffleBlockInfoList);

  void registerShuffle(ShuffleServerInfo shuffleServerInfo, String appId, int shuffleId, int start, int end);

  void sendCommit(Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId);

  void registerCoordinatorClient(String host, int port);

  ShuffleAssignmentsInfo getShuffleAssignments(
      String appId, int shuffleId, int partitionNum, int partitionsPerServer);

  void close();
}
