package org.apache.spark.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.tecent.rss.client.ShuffleServerClientManager;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Set;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.BufferManagerOptions;
import org.apache.spark.shuffle.writer.WriteBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Product2;
import scala.Some;
import scala.collection.Iterator;

public class RssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(RssShuffleWriter.class);

    private String appId;
    private int shuffleId;
    private String taskIdentify;
    private ShuffleDependency<K, V, C> shuffleDependency;
    private ShuffleWriteMetrics shuffleWriteMetrics;
    private BufferManagerOptions bufferOptions;
    private Serializer serializer;
    private Partitioner partitioner;
    private boolean shouldPartition;
    private WriteBufferManager bufferManager;
    private RssShuffleManager shuffleManager;
    private long sendCheckTimeout;
    private long sendCheckInterval;
    private Set<ShuffleServerInfo> shuffleServerInfos;

    public RssShuffleWriter(String appId, int shuffleId, int executorId, String taskIdentify,
            ShuffleWriteMetrics shuffleWriteMetrics,
            BufferManagerOptions bufferOptions, RssShuffleHandle rssShuffleHandle,
            RssShuffleManager shuffleManager, long sendCheckTimeout, long sendCheckInterval) {
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.taskIdentify = taskIdentify;
        this.shuffleDependency = rssShuffleHandle.getDependency();
        this.shuffleWriteMetrics = shuffleWriteMetrics;
        this.bufferOptions = bufferOptions;
        this.serializer = shuffleDependency.serializer();
        this.partitioner = shuffleDependency.partitioner();
        this.shuffleManager = shuffleManager;
        this.shouldPartition = partitioner.numPartitions() > 1;
        this.shuffleServerInfos = Sets.newConcurrentHashSet();
        this.sendCheckInterval = sendCheckInterval;
        this.sendCheckTimeout = sendCheckTimeout;
        bufferManager = new WriteBufferManager(shuffleId, bufferOptions.getIndividualBufferSize(),
                bufferOptions.getIndividualBufferMax(), bufferOptions.getBufferSpillThreshold(),
                executorId, serializer, rssShuffleHandle.getPartitionToServers());
    }

    @Override
    public void write(Iterator<Product2<K, V>> records) {
        List<ShuffleBlockInfo> spilledData = null;
        Set<Long> blockIds = Sets.newConcurrentHashSet();
        while (records.hasNext()) {
            Product2<K, V> record = records.next();
            int partition = getPartition(record._1());
            if (shuffleDependency.mapSideCombine()) {
                // doesn't support for now
            } else {
                spilledData = bufferManager.addRecord(partition, record._1(), record._2());
            }

            if (spilledData != null && !spilledData.isEmpty()) {
                shuffleManager.getEventLoop().post(
                        new AddBlockEvent(taskIdentify, spilledData));
                // add blockId to set, check if it is send later
                spilledData.parallelStream().forEach(sbi -> {
                    blockIds.add(sbi.getBlockId());
                    shuffleServerInfos.addAll(sbi.getShuffleServerInfos());
                });
            }
        }

        spilledData = bufferManager.clear();
        shuffleManager.getEventLoop()
                .post(new AddBlockEvent(taskIdentify, spilledData));
        spilledData.parallelStream().forEach(sbi -> {
            blockIds.add(sbi.getBlockId());
            shuffleServerInfos.addAll(sbi.getShuffleServerInfos());
        });

        checkBlockSendResult(blockIds);

        sendCommit();
    }

    @VisibleForTesting
    protected void sendCommit() {
        shuffleServerInfos.parallelStream().forEach(ssi -> {
            ShuffleServerClientManager.getInstance()
                    .getClient(ssi).commitShuffleTask(appId, shuffleId);
        });
    }

    @VisibleForTesting
    protected void checkBlockSendResult(Set<Long> blockIds) throws RuntimeException {
        long start = System.currentTimeMillis();
        while (true) {
            Set<Long> failedBlockIds = shuffleManager.getFailedBlockIds(taskIdentify);
            Set<Long> successBlockIds = shuffleManager.getSuccessBlockIds(taskIdentify);
            // if failed when send data to shuffle server, mark task as failed
            if (failedBlockIds.size() > 0) {
                String errorMsg =
                        "Send failed: Task[" + taskIdentify + "] failed because blockIds ["
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
                        "Timeout: Task[" + taskIdentify + "] failed because blockIds [" + Joiner.on(" ").join(blockIds)
                                + "] can't be sent to shuffle server in " + (sendCheckTimeout / 1000) + "s.";
                LOG.error(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        return new Some<MapStatus>(null);
    }

    @VisibleForTesting
    protected <K> int getPartition(K key) {
        int result = 0;
        if (shouldPartition) {
            result = partitioner.getPartition(key);
        }
        return result;
    }
}
