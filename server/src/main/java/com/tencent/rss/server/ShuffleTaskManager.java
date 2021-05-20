package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleTaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleTaskManager.class);
  private final ShuffleFlushManager shuffleFlushManager;
  private AtomicLong requireBufferId = new AtomicLong(0);
  private ShuffleServerConf conf;
  private long appExpiredWithHB;
  private long appExpiredWithoutHB;
  private long preAllocationExpired;
  private long commitCheckInterval;
  // appId -> shuffleId -> partitionId -> taskAttemptId -> blockIds to avoid too many appId
  // store taskAttemptId info to filter speculation task
  private Map<String, Map<Integer, Map<Integer, Map<Long, long[]>>>> partitionsToBlockIds;
  private ShuffleBufferManager shuffleBufferManager;
  private Map<String, Long> appIds = Maps.newConcurrentMap();
  // appId -> shuffleId -> commit count
  private Map<String, Map<Long, Integer>> commitCounts = Maps.newHashMap();
  // appId -> shuffleId -> shuffle block count
  private Map<String, Map<Long, Integer>> cachedBlockCount = Maps.newHashMap();
  private Map<Long, PreAllocatedBufferInfo> requireBufferIds = Maps.newConcurrentMap();
  private ScheduledExecutorService scheduledExecutorService;

  public ShuffleTaskManager(
      ShuffleServerConf conf,
      ShuffleFlushManager shuffleFlushManager,
      ShuffleBufferManager shuffleBufferManager) {
    this.conf = conf;
    this.shuffleFlushManager = shuffleFlushManager;
    this.partitionsToBlockIds = Maps.newConcurrentMap();
    this.shuffleBufferManager = shuffleBufferManager;
    this.appExpiredWithHB = conf.getLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITH_HEARTBEAT);
    this.appExpiredWithoutHB = conf.getLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT);
    this.commitCheckInterval = conf.getLong(ShuffleServerConf.SERVER_COMMIT_CHECK_INTERVAL);
    this.preAllocationExpired = conf.getLong(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED);
    // the thread for checking application status
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(
        () -> preAllocatedBufferCheck(), preAllocationExpired / 2,
        preAllocationExpired / 2, TimeUnit.MILLISECONDS);
  }

  public StatusCode registerShuffle(String appId, int shuffleId, int startPartition, int endPartition) {
    refreshAppId(appId);
    return shuffleBufferManager.registerBuffer(appId, shuffleId, startPartition, endPartition);
  }

  public StatusCode cacheShuffleData(String appId, int shuffleId, boolean isPreAllocated, ShufflePartitionedData spd) {
    refreshAppId(appId);
    return shuffleBufferManager.cacheShuffleData(appId, shuffleId, isPreAllocated, spd);
  }

  public boolean isPreAllocated(long requireBufferId) {
    return requireBufferIds.containsKey(requireBufferId);
  }

  public void removeRequireBufferId(long requireId) {
    requireBufferIds.remove(requireId);
  }

  public StatusCode commitShuffle(String appId, int shuffleId) throws Exception {
    long start = System.currentTimeMillis();
    refreshAppId(appId);
    shuffleBufferManager.commitShuffleTask(appId, shuffleId);
    long commitTimeout = conf.get(ShuffleServerConf.SERVER_COMMIT_TIMEOUT);
    int expectedCommitted = getCachedBlockCount(appId, shuffleId);
    while (true) {
      int committedBlockCount = shuffleFlushManager.getCommittedBlockCount(appId, shuffleId);
      if (committedBlockCount >= expectedCommitted) {
        break;
      }
      Thread.sleep(commitCheckInterval);
      if (System.currentTimeMillis() - start > commitTimeout) {
        throw new RuntimeException("Shuffle data commit timeout for " + commitTimeout + " ms");
      }
      LOG.info("Checking commit result for appId[" + appId + "], shuffleId[" + shuffleId
          + "], expect committed[" + expectedCommitted + "], actual committed[" + committedBlockCount + "]");
    }

    return StatusCode.SUCCESS;
  }

  public synchronized void addFinishedBlockIds(
      String appId, Integer shuffleId, Long taskAttemptId, Map<Integer, long[]> partitionToBlockIds) {
    refreshAppId(appId);
    partitionsToBlockIds.putIfAbsent(appId, Maps.newConcurrentMap());
    Map<Integer, Map<Integer, Map<Long, long[]>>> shuffleToPartitions = partitionsToBlockIds.get(appId);
    shuffleToPartitions.putIfAbsent(shuffleId, Maps.newConcurrentMap());
    Map<Integer, Map<Long, long[]>> existPartitionToBlockIds = shuffleToPartitions.get(shuffleId);
    for (Map.Entry<Integer, long[]> entry : partitionToBlockIds.entrySet()) {
      Integer partitionId = entry.getKey();
      existPartitionToBlockIds.putIfAbsent(partitionId, Maps.newConcurrentMap());
      existPartitionToBlockIds.get(partitionId).put(taskAttemptId, entry.getValue());
    }
  }

  public synchronized int updateAndGetCommitCount(String appId, long shuffleId) {
    if (!commitCounts.containsKey(appId)) {
      commitCounts.put(appId, Maps.newHashMap());
    }
    Map<Long, Integer> shuffleCommit = commitCounts.get(appId);
    if (!shuffleCommit.containsKey(shuffleId)) {
      shuffleCommit.put(shuffleId, 0);
    }
    int commitNum = shuffleCommit.get(shuffleId) + 1;
    shuffleCommit.put(shuffleId, commitNum);
    return commitNum;
  }

  public synchronized void updateCachedBlockCount(String appId, long shuffleId, int blockNum) {
    if (!cachedBlockCount.containsKey(appId)) {
      cachedBlockCount.put(appId, Maps.newHashMap());
    }
    Map<Long, Integer> cachedBlocks = cachedBlockCount.get(appId);
    if (!cachedBlocks.containsKey(shuffleId)) {
      cachedBlocks.put(shuffleId, 0);
    }
    cachedBlocks.put(shuffleId, cachedBlocks.get(shuffleId) + blockNum);
  }

  public int getCachedBlockCount(String appId, long shuffleId) {
    if (!cachedBlockCount.containsKey(appId)) {
      return 0;
    }
    Map<Long, Integer> cachedBlocks = cachedBlockCount.get(appId);
    if (!cachedBlocks.containsKey(shuffleId)) {
      return 0;
    }
    return cachedBlocks.get(shuffleId);
  }

  public long requireBuffer(int requireSize) {
    long requireId = -1;
    if (shuffleBufferManager.requireMemory(requireSize, true)) {
      requireId = requireBufferId.incrementAndGet();
      requireBufferIds.put(requireId,
          new PreAllocatedBufferInfo(requireId, System.currentTimeMillis(), requireSize));
    }
    return requireId;
  }

  public List<Long> getFinishedBlockIds(
      String appId, Integer shuffleId, Integer partitionId, List<Long> taskAttemptIds) {
    refreshAppId(appId);
    Map<Integer, Map<Integer, Map<Long, long[]>>> shuffleToPartitions = partitionsToBlockIds.get(appId);
    if (shuffleToPartitions == null) {
      return Lists.newArrayList();
    }
    Map<Integer, Map<Long, long[]>> partitionToBlockIds = shuffleToPartitions.get(shuffleId);
    if (partitionToBlockIds == null) {
      return Lists.newArrayList();
    }
    Map<Long, long[]> taskToBlockIds = partitionToBlockIds.get(partitionId);
    List<Long> blockIds = Lists.newArrayList();
    if (taskToBlockIds != null) {
      for (Long taskAttemptId : taskAttemptIds) {
        long[] taskBlockIds = taskToBlockIds.get(taskAttemptId);
        if (taskBlockIds != null && taskBlockIds.length > 0) {
          for (long blockId : taskBlockIds) {
            blockIds.add(blockId);
          }
        }
      }
    }
    return blockIds;
  }

  public ShuffleDataResult getShuffleData(
      String appId, Integer shuffleId, Integer partitionId, int partitionNumPerRange,
      int partitionNum, int readBufferSize, String storageType, Set<Long> expectedBlockIds) {
    refreshAppId(appId);
    CreateShuffleReadHandlerRequest request = new CreateShuffleReadHandlerRequest();
    request.setAppId(appId);
    request.setShuffleId(shuffleId);
    request.setPartitionId(partitionId);
    request.setPartitionNumPerRange(partitionNumPerRange);
    request.setPartitionNum(partitionNum);
    request.setReadBufferSize(readBufferSize);
    request.setExpectedBlockIds(expectedBlockIds);
    request.setStorageType(storageType);
    request.setRssBaseConf(conf);
    return ShuffleHandlerFactory.getInstance().getServerReadHandler(request).getShuffleData(expectedBlockIds);
  }

  public void checkResourceStatus(Set<String> aliveAppIds) {
    LOG.debug("Exist appIds:" + appIds + ", aliveAppIds:" + aliveAppIds);
    Set<String> removed = Sets.newHashSet(appIds.keySet());
    removed.removeAll(aliveAppIds);
    // remove applications not in coordinator's list and timeout according to rss.server.app.expired.withoutHeartbeat
    for (String appId : removed) {
      if (System.currentTimeMillis() - appIds.get(appId) > appExpiredWithoutHB) {
        LOG.info("Remove resources of appId[" + appId + "] according "
            + "to rss.server.app.expired.withoutHeartbeat");
        removeResources(appId);
      }
    }

    // remove expired applications in coordinator's list but timeout according to rss.server.app.expired.withHeartbeat
    removed = Sets.newHashSet();
    for (Map.Entry<String, Long> entry : appIds.entrySet()) {
      if (System.currentTimeMillis() - entry.getValue() > appExpiredWithHB) {
        removed.add(entry.getKey());
      }
    }

    for (String appId : removed) {
      LOG.info("Remove resources of appId[" + appId + "] according "
          + "to rss.server.app.expired.withHeartbeat");
      removeResources(appId);
    }
  }

  private void removeResources(String appId) {
    partitionsToBlockIds.remove(appId);
    shuffleBufferManager.removeBuffer(appId);
    shuffleFlushManager.removeResources(appId);
    appIds.remove(appId);
  }

  private void refreshAppId(String appId) {
    appIds.put(appId, System.currentTimeMillis());
  }

  // check pre allocated buffer, release the memory if it expired
  private void preAllocatedBufferCheck() {
    long current = System.currentTimeMillis();
    List<Long> removeIds = Lists.newArrayList();
    for (PreAllocatedBufferInfo info : requireBufferIds.values()) {
      if (current - info.getTimestamp() > preAllocationExpired) {
        removeIds.add(info.getRequireId());
        shuffleBufferManager.releaseMemory(info.getRequireSize(), false, true);
      }
    }
    for (Long requireId : removeIds) {
      requireBufferIds.remove(requireId);
      LOG.info("Remove expired requireId " + requireId);
    }
  }

  public int getRequireBufferSize(long requireId) {
    PreAllocatedBufferInfo pabi = requireBufferIds.get(requireId);
    if (pabi == null) {
      return 0;
    }
    return pabi.getRequireSize();
  }

  @VisibleForTesting
  public Map<String, Long> getAppIds() {
    return appIds;
  }

  @VisibleForTesting
  Map<Long, PreAllocatedBufferInfo> getRequireBufferIds() {
    return requireBufferIds;
  }
}
