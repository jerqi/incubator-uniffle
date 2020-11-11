package org.apache.spark.shuffle;

import com.tecent.rss.client.BufferManagerOptions;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.internal.Logging;

public class RssShuffleManager implements ShuffleManager {

    // This method is called in Spark driver side,
    // and Spark driver will make some decision according to coordinator,
    // e.g. determining what RSS servers to use.
    // Then Spark driver will return a ShuffleHandle and
    // pass that ShuffleHandle to executors (getWriter/getReader).
    @Override
    public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {
        return new RssShuffleHandle(shuffleId, "appId", numMaps, dependency, null);
    }

    // This method is called in Spark executor,
    // getting information from Spark driver via the ShuffleHandle.
    @Override
    public <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId,
            TaskContext context) {
        SparkEnv.get().executorId();
        if (handle instanceof RssShuffleHandle) {
            return new RssShuffleWriter(((RssShuffleHandle) handle).getNumMaps(),
                    ((RssShuffleHandle) handle).getDependency(), context.taskMetrics().shuffleWriteMetrics(),
                    new BufferManagerOptions(0, 0, 0));
        } else {
            return null;
        }
    }

    // This method is called in Spark executor,
    // getting information from Spark driver via the ShuffleHandle.
    @Override
    public <K, C> ShuffleReader<K, C> getReader(ShuffleHandle handle,
            int startPartition, int endPartition, TaskContext context) {
        if (handle instanceof RssShuffleHandle) {
            return new RssShuffleReader(0, 0, context, null,
                    ((RssShuffleHandle) handle).getDependency(), ((RssShuffleHandle) handle).getNumMaps(), 0);
        } else {
            return null;
        }
    }

    public <K, C> ShuffleReader<K, C> getReader(ShuffleHandle handle, int startPartition,
            int endPartition, TaskContext context, int startMapId, int endMapId) {
        return null;
    }

    @Override
    public boolean unregisterShuffle(int shuffleId) {
        return true;
    }

    @Override
    public void stop() {
    }

    @Override
    public ShuffleBlockResolver shuffleBlockResolver() {
        throw new RuntimeException("RssShuffleManager.shuffleBlockResolver is not implemented");
    }
}
