package com.tencent.rss.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleTaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleTaskManager.class);
  private final ShuffleFlushManager shuffleFlushManager;
  private ShuffleServerConf conf;
  // appId -> shuffleId -> partitionId -> blockIds to avoid too many appId
  private Map<String, Map<Integer, Map<Integer, List<Long>>>> partitionsToBlockIds;
  private ShuffleBufferManager shuffleBufferManager;

  public ShuffleTaskManager(
      ShuffleServerConf conf,
      ShuffleFlushManager shuffleFlushManager,
      ShuffleBufferManager shuffleBufferManager) {
    this.conf = conf;
    this.shuffleFlushManager = shuffleFlushManager;
    this.partitionsToBlockIds = Maps.newConcurrentMap();
    this.shuffleBufferManager = shuffleBufferManager;
  }

  public StatusCode registerShuffle(String appId, int shuffleId, int startPartition, int endPartition) {
    return shuffleBufferManager.registerBuffer(appId, shuffleId, startPartition, endPartition);
  }

  public StatusCode cacheShuffleData(String appId, int shuffleId, ShufflePartitionedData spd) {
    return shuffleBufferManager.cacheShuffleData(appId, shuffleId, spd);
  }

  public synchronized StatusCode commitShuffle(String appId, int shuffleId) throws Exception {
    RangeMap<Integer, Set<Long>> partitionToEventIds = shuffleBufferManager.commitShuffleTask(appId, shuffleId);

    long commitTimeout = conf.get(ShuffleServerConf.SERVER_COMMIT_TIMEOUT);
    long start = System.currentTimeMillis();
    while (true) {
      List<Range<Integer>> removedRanges = Lists.newArrayList();
      for (Map.Entry<Range<Integer>, Set<Long>> entry : partitionToEventIds.asMapOfRanges().entrySet()) {
        Range<Integer> range = entry.getKey();
        Set<Long> committedIds = shuffleFlushManager.getEventIds(appId, shuffleId, range);
        if (committedIds != null && !committedIds.isEmpty()) {
          Set<Long> expectedEventIds = entry.getValue();
          LOG.debug("Got expectedEventIds for appId[" + appId + "], shuffleId[" + shuffleId + "], partitionRange["
              + range + "], " + expectedEventIds + ", current commit: " + committedIds);
          expectedEventIds.removeAll(committedIds);
          if (expectedEventIds.isEmpty()) {
            removedRanges.add(range);
          }
        }
      }
      for (Range<Integer> range : removedRanges) {
        partitionToEventIds.remove(range);
        LOG.info("appId[" + appId + "], shuffleId[" + shuffleId + "], partitionRange["
            + range + "] is committed for current request.");
      }
      if (partitionToEventIds.asMapOfRanges().isEmpty()) {
        break;
      }
      Thread.sleep(1000);
      if (System.currentTimeMillis() - start > commitTimeout) {
        throw new RuntimeException("Shuffle data commit timeout for " + commitTimeout + " ms");
      }
      LOG.info("Checking commit result for appId[" + appId + "], shuffleId[" + shuffleId + "]");
    }

    return StatusCode.SUCCESS;
  }

  public synchronized void addFinishedBlockIds(
      String appId, Integer shuffleId, Map<Integer, List<Long>> partitionToBlockIds) {
    partitionsToBlockIds.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Integer, Map<Integer, List<Long>>> shuffleToPartitions = partitionsToBlockIds.get(appId);
    shuffleToPartitions.putIfAbsent(shuffleId, Maps.newConcurrentMap());
    Map<Integer, List<Long>> existPartitionToBlockIds = shuffleToPartitions.get(shuffleId);
    for (Map.Entry<Integer, List<Long>> entry : partitionToBlockIds.entrySet()) {
      Integer partitionId = entry.getKey();
      existPartitionToBlockIds.putIfAbsent(partitionId, Lists.newArrayList());
      existPartitionToBlockIds.get(partitionId).addAll(entry.getValue());
    }
  }

  public List<Long> getFinishedBlockIds(String appId, Integer shuffleId, Integer partitionId) {
    Map<Integer, Map<Integer, List<Long>>> shuffleToPartitions = partitionsToBlockIds.get(appId);
    if (shuffleToPartitions == null) {
      return Lists.newArrayList();
    }
    Map<Integer, List<Long>> partitionToBlockIds = shuffleToPartitions.get(shuffleId);
    if (partitionToBlockIds == null) {
      return Lists.newArrayList();
    }
    List<Long> blockIds = partitionToBlockIds.get(partitionId);
    if (blockIds == null) {
      return Lists.newArrayList();
    }
    return blockIds;
  }

  public ShuffleDataResult getShuffleData(
      String appId, Integer shuffleId, Integer partitionId, int partitionsPerServer,
      int partitionNum, int readBufferSize, String storageType, Set<Long> expectedBlockIds) {
    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setAppId(appId);
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setPartitionsPerServer(partitionsPerServer);
    request.setPartitionNum(partitionNum);
    request.setReadBufferSize(readBufferSize);
    request.setExpectedBlockIds(expectedBlockIds);
    request.setStorageType(storageType);
    request.setRssBaseConf(conf);
    return ShuffleHandlerFactory.getInstance().getServerReadHandler(request).getShuffleData(expectedBlockIds);
  }

}
