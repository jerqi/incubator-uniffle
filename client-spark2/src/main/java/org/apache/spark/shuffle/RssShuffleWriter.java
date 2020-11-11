package org.apache.spark.shuffle;

import com.tecent.rss.client.BufferManagerOptions;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.Logging;
import org.apache.spark.scheduler.MapStatus;
import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Some;
import scala.collection.Iterator;

public class RssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

    private int numMaps;
    private ShuffleDependency<K, V, C> shuffleDependency;
    private ShuffleWriteMetrics shuffleWriteMetrics;
    private BufferManagerOptions bufferOptions;

    public RssShuffleWriter(int numMaps, ShuffleDependency<K, V, C> shuffleDependency,
            ShuffleWriteMetrics shuffleWriteMetrics, BufferManagerOptions bufferOptions) {
        this.numMaps = numMaps;
        this.shuffleDependency = shuffleDependency;
        this.shuffleWriteMetrics = shuffleWriteMetrics;
        this.bufferOptions = bufferOptions;
    }

    @Override
    public void write(Iterator<Product2<K, V>> records) {
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        return new Some<MapStatus>(null);
    }

    public int getNumMaps() {
        return numMaps;
    }

    public void setNumMaps(int numMaps) {
        this.numMaps = numMaps;
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
}
