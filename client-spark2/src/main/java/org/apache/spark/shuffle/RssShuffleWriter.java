package org.apache.spark.shuffle;

import com.tencent.rss.proto.RssProtos;
import java.util.Map;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.BufferManagerOptions;
import org.apache.spark.shuffle.writer.WriteBufferManager;
import scala.Option;
import scala.Product2;
import scala.Some;
import scala.collection.Iterator;

public class RssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

    private int appId;
    private int shuffleId;
    private int executorId;
    private long taskAttempId;
    private ShuffleDependency<K, V, C> shuffleDependency;
    private ShuffleWriteMetrics shuffleWriteMetrics;
    private BufferManagerOptions bufferOptions;
    private Serializer serializer;
    private Partitioner partitioner;
    private boolean shouldPartition;
    private WriteBufferManager bufferManager;

    public RssShuffleWriter(int shuffleId, int executorId, long taskAttempId,
            ShuffleDependency<K, V, C> shuffleDependency, ShuffleWriteMetrics shuffleWriteMetrics,
            BufferManagerOptions bufferOptions, Serializer serializer) {
        this.shuffleId = shuffleId;
        this.taskAttempId = taskAttempId;
        this.shuffleDependency = shuffleDependency;
        this.shuffleWriteMetrics = shuffleWriteMetrics;
        this.bufferOptions = bufferOptions;
        this.serializer = serializer;
        this.partitioner = shuffleDependency.partitioner();
        this.shouldPartition = this.partitioner.numPartitions() > 1;
        bufferManager = new WriteBufferManager(bufferOptions.getIndividualBufferSize(),
                bufferOptions.getIndividualBufferMax(), bufferOptions.getBufferSpillThreshold(),
                executorId, serializer);
    }

    @Override
    public void write(Iterator<Product2<K, V>> records) {
        Map<Integer, RssProtos.ShuffleBlock> spilledData = null;
        RssShuffleManager shuffleManager = (RssShuffleManager) SparkEnv.get().shuffleManager();
        while (records.hasNext()) {
            Product2<K, V> record = records.next();
            int partition = getPartition(record._1());
            if (shuffleDependency.mapSideCombine()) {
                // doesn't support for now
            } else {
                spilledData = bufferManager.addRecord(partition, record._1(), record._2());
            }

            if (!spilledData.isEmpty()) {
                shuffleManager.getEventLoop().post(new AddBlockEvent(taskAttempId, spilledData));
            }
        }
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        return new Some<MapStatus>(null);
    }

    private <K> int getPartition(K key) {
        int result = 0;
        if (shouldPartition) {
            result = partitioner.getPartition(key);
        }
        return result;
    }

    public ShuffleDependency<K, V, C> getShuffleDependency() {
        return shuffleDependency;
    }

    public void setShuffleDependency(ShuffleDependency<K, V, C> shuffleDependency) {
        this.shuffleDependency = shuffleDependency;
    }

    public ShuffleWriteMetrics getShuffleWriteMetrics() {
        return shuffleWriteMetrics;
    }

    public void setShuffleWriteMetrics(ShuffleWriteMetrics shuffleWriteMetrics) {
        this.shuffleWriteMetrics = shuffleWriteMetrics;
    }

    public BufferManagerOptions getBufferOptions() {
        return bufferOptions;
    }

    public void setBufferOptions(BufferManagerOptions bufferOptions) {
        this.bufferOptions = bufferOptions;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }
}
