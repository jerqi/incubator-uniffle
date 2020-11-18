package org.apache.spark.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.tecent.rss.client.ClientUtils;
import com.tencent.rss.common.CoordinatorGrpcClient;
import com.tencent.rss.common.ShuffleServerHandler;
import com.tencent.rss.proto.RssProtos;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.BufferManagerOptions;
import org.apache.spark.util.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssShuffleManager implements ShuffleManager {

    private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
    private SparkConf sparkConf;
    private String appId;

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

    public RssShuffleManager(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
        appId = SparkContext.getOrCreate().applicationId();
    }

    // This method is called in Spark driver side,
    // and Spark driver will make some decision according to coordinator,
    // e.g. determining what RSS servers to use.
    // Then Spark driver will return a ShuffleHandle and
    // pass that ShuffleHandle to executors (getWriter/getReader).
    @Override
    public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {
        CoordinatorGrpcClient coordinatorClient = getCoordinatorClient();
        // ask coordinator for ShuffleServerHandler
        int partitionsPerServer = sparkConf.getInt(RssClientConfig.RSS_PARTITIONS_PER_SERVER,
                RssClientConfig.RSS_PARTITIONS_PER_SERVER_DEFAULT_VALUE);

        RssProtos.GetShuffleAssignmentsResponse response = coordinatorClient
                .getShuffleAssignments(appId, shuffleId, numMaps, partitionsPerServer);

        ShuffleServerHandler shuffleServerHandler = ClientUtils.toShuffleServerHandler(response);

        return new RssShuffleHandle(shuffleId, appId, numMaps, dependency, shuffleServerHandler);
    }

    @VisibleForTesting
    protected CoordinatorGrpcClient getCoordinatorClient() {
        return new CoordinatorGrpcClient(sparkConf.get(RssClientConfig.RSS_COORDINATOR_IP),
                sparkConf.getInt(RssClientConfig.RSS_COORDINATOR_PORT,
                        RssClientConfig.RSS_COORDINATOR_PORT_DEFAULT_VALUE));
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
                    new BufferManagerOptions(sparkConf),
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
