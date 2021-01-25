package org.apache.spark.shuffle.writer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.util.ClientUtils;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.Some;
import scala.collection.Iterator;

public class RssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleWriter.class);

  private static final String DUMMY_HOST = "dummy_host";
  private static final int DUMMY_PORT = 99999;
  private String appId;
  private int shuffleId;
  private String taskId;
  private ShuffleDependency<K, V, C> shuffleDependency;
  private ShuffleWriteMetrics shuffleWriteMetrics;
  private Partitioner partitioner;
  private boolean shouldPartition;
  private WriteBufferManager bufferManager;
  private RssShuffleManager shuffleManager;
  private long sendCheckTimeout;
  private long sendCheckInterval;
  private Set<ShuffleServerInfo> shuffleServerInfoSet;
  private Map<Integer, Set<Long>> partitionToBlockIds;
  private ShuffleWriteClient shuffleWriteClient;

  public RssShuffleWriter(
      String appId,
      int shuffleId,
      String taskId,
      WriteBufferManager bufferManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      ShuffleDependency shuffleDependency,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient) {
    this.appId = appId;
    this.bufferManager = bufferManager;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.shuffleDependency = shuffleDependency;
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.partitioner = shuffleDependency.partitioner();
    this.shuffleManager = shuffleManager;
    this.shouldPartition = partitioner.numPartitions() > 1;
    this.shuffleServerInfoSet = Sets.newConcurrentHashSet();
    this.sendCheckTimeout = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT,
        RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT_DEFAULT_VALUE);
    this.sendCheckInterval = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL,
        RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL_DEFAULT_VALUE);
    this.partitionToBlockIds = Maps.newConcurrentMap();
    this.shuffleWriteClient = shuffleWriteClient;
  }

  /**
   * Create dummy BlockManagerId and embed partition->blockIds
   */
  private BlockManagerId createDummyBlockManagerId(String executorId, String topologyInfo) {
    // dummy values are used there for host and port check in BlockManagerId
    // hack: use topologyInfo field in BlockManagerId to store [partition, blockIds]
    return BlockManagerId.apply(executorId, DUMMY_HOST, DUMMY_PORT, Some.apply(topologyInfo));
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) {
    List<ShuffleBlockInfo> shuffleBlockInfos = null;
    Set<Long> blockIds = Sets.newConcurrentHashSet();
    final long startWrite = System.currentTimeMillis();
    while (records.hasNext()) {
      Product2<K, V> record = records.next();
      int partition = getPartition(record._1());
      if (shuffleDependency.mapSideCombine()) {
        Function1 createCombiner = shuffleDependency.aggregator().get().createCombiner();
        Object c = createCombiner.apply(record._2());
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), c);
      } else {
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), record._2());
      }
      processShuffleBlockInfos(shuffleBlockInfos, blockIds);
    }

    shuffleBlockInfos = bufferManager.clear();
    processShuffleBlockInfos(shuffleBlockInfos, blockIds);
    checkBlockSendResult(blockIds);

    sendCommit();
    long writeDuration = System.currentTimeMillis() - startWrite;
    shuffleWriteMetrics.incWriteTime(writeDuration);
    LOG.info("Finish write shuffle for appId[" + appId + "], shuffleId[" + shuffleId
        + "], taskId[" + taskId + "] with " + writeDuration + " ms");
  }

  /**
   * ShuffleBlock will be added to queue and send to shuffle server
   * maintenance the following information:
   * 1. add blockId to set, check if it is send later
   * 2. update shuffle server info, they will be used in commit phase
   * 3. update [partition, blockIds], it will be set to MapStatus,
   * and shuffle reader will do the integration check with them
   *
   * @param shuffleBlockInfos
   * @param blockIds
   */
  private void processShuffleBlockInfos(List<ShuffleBlockInfo> shuffleBlockInfos, Set<Long> blockIds) {
    if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
      shuffleBlockInfos.parallelStream().forEach(sbi -> {
        long blockId = sbi.getBlockId();
        // add blockId to set, check if it is send later
        blockIds.add(blockId);
        LOG.info("Block ready to queue " + sbi.toString());
        // update shuffle server info, they will be used in commit phase
        shuffleServerInfoSet.addAll(sbi.getShuffleServerInfos());
        // update [partition, blockIds], it will be set to MapStatus
        int partitionId = sbi.getPartitionId();
        if (partitionToBlockIds.get(partitionId) == null) {
          partitionToBlockIds.put(partitionId, Sets.newConcurrentHashSet());
        }
        partitionToBlockIds.get(partitionId).add(blockId);
      });

      shuffleManager.getEventLoop().post(
          new AddBlockEvent(taskId, shuffleBlockInfos));
    }
  }

  @VisibleForTesting
  protected void sendCommit() {
    shuffleWriteClient.sendCommit(shuffleServerInfoSet, appId, shuffleId);
  }

  @VisibleForTesting
  protected void checkBlockSendResult(Set<Long> blockIds) throws RuntimeException {
    long start = System.currentTimeMillis();
    while (true) {
      Set<Long> failedBlockIds = shuffleManager.getFailedBlockIds(taskId);
      Set<Long> successBlockIds = shuffleManager.getSuccessBlockIds(taskId);
      // if failed when send data to shuffle server, mark task as failed
      if (failedBlockIds.size() > 0) {
        String errorMsg =
            "Send failed: Task[" + taskId + "] failed because blockIds ["
                + Joiner.on(" ").join(failedBlockIds)
                + "] can't be sent to shuffle server.";
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }

      // remove blockIds which was sent successfully, if there has none left, all data are sent
      blockIds.removeAll(successBlockIds);
      if (blockIds.isEmpty()) {
        break;
      }
      try {
        Thread.sleep(sendCheckInterval);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
        String errorMsg =
            "Timeout: Task[" + taskId + "] failed because blockIds [" + Joiner.on(" ").join(blockIds)
                + "] can't be sent to shuffle server in " + (sendCheckTimeout / 1000) + "s.";
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }
    }
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (success) {
      try {
        // fill partitionLengths with non zero dummy value so map output tracker could work correctly
        long[] partitionLengths = new long[partitioner.numPartitions()];
        Arrays.fill(partitionLengths, 1);
        String jsonStr = ClientUtils.transBlockIdsToJson(partitionToBlockIds);
        BlockManagerId blockManagerId =
            createDummyBlockManagerId(appId + "_" + taskId, jsonStr);

        for (Map.Entry<Integer, Set<Long>> entry : partitionToBlockIds.entrySet()) {
          LOG.info("Finish send blocks for shuffleId[" + shuffleId + "], partitionId["
              + entry.getKey() + "], blockIds[" + entry.getValue() + "]");
        }

        return Option.apply(MapStatus$.MODULE$.apply(blockManagerId, partitionLengths, partitionLengths));
      } catch (Exception e) {
        LOG.error("Error when create DummyBlockManagerId.", e);
        throw new RuntimeException(e);
      }
    } else {
      return Option.empty();
    }
  }

  @VisibleForTesting
  protected <K> int getPartition(K key) {
    int result = 0;
    if (shouldPartition) {
      result = partitioner.getPartition(key);
    }
    return result;
  }

  @VisibleForTesting
  protected Map<Integer, Set<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }

  @VisibleForTesting
  protected ShuffleWriteMetrics getShuffleWriteMetrics() {
    return shuffleWriteMetrics;
  }
}
