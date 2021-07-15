package com.tencent.rss.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.factory.ShuffleHandlerFactory;
import com.tencent.rss.storage.request.CreateShuffleReadHandlerRequest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleTaskManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleTaskManager.class);
  private final ShuffleFlushManager shuffleFlushManager;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ScheduledExecutorService expiredAppCleanupExecutorService;
  private AtomicLong requireBufferId = new AtomicLong(0);
  private ShuffleServerConf conf;
  private long appExpiredWithoutHB;
  private long preAllocationExpired;
  private long commitCheckInterval;
  // appId -> shuffleId -> partitionId -> blockIds to avoid too many appId
  // store taskAttemptId info to filter speculation task
  private Map<String, Map<Integer, Map<Integer, Roaring64NavigableMap>>> partitionsToBlockIds;
  private ShuffleBufferManager shuffleBufferManager;
  private Map<String, Long> appIds = Maps.newConcurrentMap();
  // appId -> shuffleId -> commit count
  private Map<String, Map<Long, Integer>> commitCounts = Maps.newHashMap();
  // appId -> shuffleId -> shuffle block count
  private Map<String, Map<Long, Integer>> cachedBlockCount = Maps.newHashMap();
  private Map<Long, PreAllocatedBufferInfo> requireBufferIds = Maps.newConcurrentMap();
  private Runnable clearResourceThread;
  private BlockingQueue<String> expiredAppIdQueue = Queues.newLinkedBlockingQueue();

  public ShuffleTaskManager(
      ShuffleServerConf conf,
      ShuffleFlushManager shuffleFlushManager,
      ShuffleBufferManager shuffleBufferManager) {
    this.conf = conf;
    this.shuffleFlushManager = shuffleFlushManager;
    this.partitionsToBlockIds = Maps.newConcurrentMap();
    this.shuffleBufferManager = shuffleBufferManager;
    this.appExpiredWithoutHB = conf.getLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT);
    this.commitCheckInterval = conf.getLong(ShuffleServerConf.SERVER_COMMIT_CHECK_INTERVAL);
    this.preAllocationExpired = conf.getLong(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED);
    // the thread for checking application status
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(
        () -> preAllocatedBufferCheck(), preAllocationExpired / 2,
        preAllocationExpired / 2, TimeUnit.MILLISECONDS);
    this.expiredAppCleanupExecutorService = Executors.newSingleThreadScheduledExecutor();
    expiredAppCleanupExecutorService.scheduleAtFixedRate(
        () -> checkResourceStatus(), appExpiredWithoutHB / 2,
        appExpiredWithoutHB / 2, TimeUnit.MILLISECONDS);
    // the thread for clear expired resources
    clearResourceThread = () -> {
      try {
        while (true) {
          String appId = expiredAppIdQueue.take();
          removeResources(appId);
        }
      } catch (Exception e) {
        LOG.error("Exception happened when clear resource for expired application", e);
      }
    };
    new Thread(clearResourceThread).start();
  }

  public StatusCode registerShuffle(String appId, int shuffleId, List<PartitionRange> partitionRanges) {
    partitionsToBlockIds.putIfAbsent(appId, Maps.newConcurrentMap());
    for (PartitionRange partitionRange : partitionRanges) {
      shuffleBufferManager.registerBuffer(appId, shuffleId, partitionRange.getStart(), partitionRange.getEnd());
    }
    return StatusCode.SUCCESS;
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
    LOG.info("Finish commit " + expectedCommitted + " blocks for appId[" + appId
        + "], shuffleId[" + shuffleId + "] cost " + (System.currentTimeMillis() - start) + " ms to check");

    return StatusCode.SUCCESS;
  }

  public synchronized void addFinishedBlockIds(
      String appId, Integer shuffleId, Map<Integer, long[]> partitionToBlockIds) {
    refreshAppId(appId);
    Map<Integer, Map<Integer, Roaring64NavigableMap>> shuffleIdToPartitions = partitionsToBlockIds.get(appId);
    shuffleIdToPartitions.putIfAbsent(shuffleId, Maps.newConcurrentMap());
    Map<Integer, Roaring64NavigableMap> partitionIdToBlockIds = shuffleIdToPartitions.get(shuffleId);
    for (Map.Entry<Integer, long[]> entry : partitionToBlockIds.entrySet()) {
      Integer partitionId = entry.getKey();
      partitionIdToBlockIds.putIfAbsent(partitionId, Roaring64NavigableMap.bitmapOf());
      Roaring64NavigableMap provider = partitionIdToBlockIds.get(partitionId);
      for (long blockId : entry.getValue()) {
        provider.addLong(blockId);
      }
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

  public byte[] getFinishedBlockIds(
      String appId, Integer shuffleId, Integer partitionId) throws IOException {
    refreshAppId(appId);
    Map<Integer, Map<Integer, Roaring64NavigableMap>> shuffleIdToPartitions = partitionsToBlockIds.get(appId);
    if (shuffleIdToPartitions == null) {
      return null;
    }
    Map<Integer, Roaring64NavigableMap> partitionIdToBlockIds = shuffleIdToPartitions.get(shuffleId);
    if (partitionIdToBlockIds == null) {
      return new byte[]{};
    }
    Roaring64NavigableMap bitmap = partitionIdToBlockIds.get(partitionId);
    if (bitmap == null) {
      return new byte[]{};
    }
    return RssUtils.serializeBitMap(bitmap);
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

  public void checkResourceStatus() {
    Set<String> removed = Sets.newHashSet(appIds.keySet());
    // remove applications not in coordinator's list and timeout according to rss.server.app.expired.withoutHeartbeat
    for (String appId : removed) {
      if (System.currentTimeMillis() - appIds.get(appId) > appExpiredWithoutHB) {
        LOG.info("Detect expired appId[" + appId + "] according "
            + "to rss.server.app.expired.withoutHeartbeat");
        expiredAppIdQueue.add(appId);
      }
    }
  }

  private void removeResources(String appId) {
    LOG.info("Start remove resource for appId[" + appId + "]");
    final long start = System.currentTimeMillis();
    appIds.remove(appId);
    partitionsToBlockIds.remove(appId);
    shuffleBufferManager.removeBuffer(appId);
    shuffleFlushManager.removeResources(appId);
    LOG.info("Finish remove resource for appId[" + appId + "] cost " + (System.currentTimeMillis() - start) + " ms");
  }

  public void refreshAppId(String appId) {
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
