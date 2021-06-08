package com.tencent.rss.client.api;

import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ShuffleWriteClient {

  SendShuffleDataResult sendShuffleData(String appId, List<ShuffleBlockInfo> shuffleBlockInfoList);

  void sendAppHeartbeat(String appId, long timeoutMs);

  void registerShuffle(ShuffleServerInfo shuffleServerInfo, String appId, int shuffleId, int start, int end);

  void sendCommit(Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps);

  void registerCoordinators(String coordinators);

  void reportShuffleResult(Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId, int shuffleId, long taskAttemptId, Map<Integer, List<Long>> partitionToBlockIds);

  ShuffleAssignmentsInfo getShuffleAssignments(
      String appId, int shuffleId, int partitionNum, int partitionNumPerRange, int dataReplica);

  List<Long> getShuffleResult(String clientType, Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId, int shuffleId, int partitionId, List<Long> taskAttemptIds);

  void close();
}
