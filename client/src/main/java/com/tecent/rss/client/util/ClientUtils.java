package com.tecent.rss.client.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientUtils {

  private static AtomicInteger ATOMIC_INT = new AtomicInteger(0);

  // BlockId is long and composed by executorId and AtomicInteger
  // executorId is high-32 bit and AtomicInteger is low-32 bit
  public static Long getBlockId(long executorId, int atomicInt) {
    if (atomicInt < 0) {
      throw new RuntimeException("Block size is out of scope which is " + Integer.MAX_VALUE);
    }
    return (executorId << 32) + atomicInt;
  }

  public static int getAtomicInteger() {
    return ATOMIC_INT.getAndIncrement();
  }

  public static String transBlockIdsToJson(Map<Integer, Set<Long>> partitionToBlockIds)
      throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(partitionToBlockIds);
  }

  public static Map<Integer, Set<Long>> getBlockIdsFromJson(String jsonStr) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<HashMap<Integer, Set<Long>>> typeRef
        = new TypeReference<HashMap<Integer, Set<Long>>>() {
    };
    return mapper.readValue(jsonStr, typeRef);
  }

  // send shuffle block to shuffle server
  // for all ShuffleBlockInfo, create the data structure as shuffleServer -> shuffleId -> partitionId -> blocks
  // it will be helpful to send rpc request to shuffleServer
  public static void getServerToBlocks(List<ShuffleBlockInfo> shuffleBlockInfoList) {
    Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks = Maps.newHashMap();
    Map<ShuffleServerInfo, List<Long>> serverToBlockIds = Maps.newHashMap();
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      int partitionId = sbi.getPartitionId();
      int shuffleId = sbi.getShuffleId();
      for (ShuffleServerInfo ssi : sbi.getShuffleServerInfos()) {
        if (!serverToBlockIds.containsKey(ssi)) {
          serverToBlockIds.put(ssi, Lists.newArrayList());
        }
        serverToBlockIds.get(ssi).add(sbi.getBlockId());

        if (!serverToBlocks.containsKey(ssi)) {
          serverToBlocks.put(ssi, Maps.newHashMap());
        }
        Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = serverToBlocks.get(ssi);
        if (!shuffleIdToBlocks.containsKey(shuffleId)) {
          shuffleIdToBlocks.put(shuffleId, Maps.newHashMap());
        }

        Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = shuffleIdToBlocks.get(shuffleId);
        if (!partitionToBlocks.containsKey(partitionId)) {
          partitionToBlocks.put(partitionId, Lists.newArrayList());
        }
        partitionToBlocks.get(partitionId).add(sbi);
      }
    }
  }
}
