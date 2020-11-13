package org.apache.spark.shuffle;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.BufferManagerOptions;
import org.apache.spark.util.EventLoop;

public class RssShuffleManager implements ShuffleManager {

    private EventLoop eventLoop = new EventLoop<AddBlockEvent>("ShuffleDataQueue") {
        @Override
        public void onReceive(AddBlockEvent event) {
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onStart() {
        }
    };

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
        if (handle instanceof RssShuffleHandle) {
            RssShuffleHandle rssHandle = (RssShuffleHandle) handle;
            return new RssShuffleWriter(rssHandle.getShuffleId(), Integer.parseInt(SparkEnv.get().executorId()),
                    context.taskAttemptId(), rssHandle.getDependency(), context.taskMetrics().shuffleWriteMetrics(),
                    new BufferManagerOptions(0, 0, 0),
                    rssHandle.getDependency().serializer());
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
                    ((RssShuffleHandle) handle).getDependency(), ((RssShuffleHandle) handle).getNumMaps(), 0,
                    ((RssShuffleHandle) handle).getDependency().serializer());
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

    public EventLoop getEventLoop() {
        return eventLoop;
    }
}
