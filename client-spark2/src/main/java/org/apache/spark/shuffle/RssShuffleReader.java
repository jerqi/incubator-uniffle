package org.apache.spark.shuffle;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.Logging;
import org.apache.spark.serializer.Serializer;
import scala.Product2;
import scala.collection.Iterator;

public class RssShuffleReader<K, V, C> implements ShuffleReader<K, C> {

    private int startPartition;
    private int endPartition;
    private TaskContext context;
    private long[] blockIds;
    private ShuffleDependency<K, V, C> shuffleDependency;
    private int numMaps;
    private int timeoutMillis;
    private Serializer serializer;

    public RssShuffleReader(int startPartition, int endPartition, TaskContext context, long[] blockIds,
            ShuffleDependency<K, V, C> shuffleDependency, int numMaps, int timeoutMillis, Serializer serializer) {
        this.startPartition = startPartition;
        this.endPartition = endPartition;
        this.context = context;
        this.blockIds = blockIds;
        this.shuffleDependency = shuffleDependency;
        this.numMaps = numMaps;
        this.timeoutMillis = timeoutMillis;
        this.serializer = serializer;
    }

    @Override
    public Iterator<Product2<K, C>> read() {
        return null;
    }

    public int getStartPartition() {
        return startPartition;
    }

    public void setStartPartition(int startPartition) {
        this.startPartition = startPartition;
    }

    public int getEndPartition() {
        return endPartition;
    }

    public void setEndPartition(int endPartition) {
        this.endPartition = endPartition;
    }

    public TaskContext getContext() {
        return context;
    }

    public void setContext(TaskContext context) {
        this.context = context;
    }

    public long[] getBlockIds() {
        return blockIds;
    }

    public void setBlockIds(long[] blockIds) {
        this.blockIds = blockIds;
    }

    public ShuffleDependency<K, V, C> getShuffleDependency() {
        return shuffleDependency;
    }

    public void setShuffleDependency(ShuffleDependency<K, V, C> shuffleDependency) {
        this.shuffleDependency = shuffleDependency;
    }

    public int getNumMaps() {
        return numMaps;
    }

    public void setNumMaps(int numMaps) {
        this.numMaps = numMaps;
    }

    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public Serializer getSerializer() {
        return serializer;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }
}
