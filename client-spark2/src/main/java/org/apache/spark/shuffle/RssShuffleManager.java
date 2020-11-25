package org.apache.spark.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tecent.rss.client.ClientUtils;
import com.tecent.rss.client.ShuffleServerClientManager;
import com.tencent.rss.common.CoordinatorGrpcClient;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerGrpcClient;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.proto.RssProtos;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
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
    private Map<String, Set<Long>> taskToFailedBlockIds;
    private Map<String, Set<Long>> taskToSuccessBlockIds;

    private EventLoop eventLoop = new EventLoop<AddBlockEvent>("ShuffleDataQueue") {
        @Override
        public void onReceive(AddBlockEvent event) {
            List<ShuffleBlockInfo> shuffleBlockInfos = event.getShuffleDataInfo();
            String taskIdentify = event.getTaskIdentify();
            // send shuffle block to shuffle server
            for (ShuffleBlockInfo sbi : shuffleBlockInfos) {
                boolean sendSuccessful = false;
                for (ShuffleServerInfo ssi : sbi.getShuffleServerInfos()) {
                    try {
                        ShuffleServerClientManager.getInstance()
                                .getClient(ssi).sendShuffleData(appId, sbi);
                        sendSuccessful = true;
                    } catch (Exception e) {
                        LOG.warn("Send shuffle data [shuffleId=" + sbi.getShuffleId() + ","
                                + " blockId=" + sbi.getBlockId()
                                + " partitionId=" + sbi.getPartitionId()
                                + " length=" + sbi.getLength() + "] to shuffle server "
                                + "[host=" + ssi.getHost()
                                + "port=" + ssi.getPort() + "] for [task="
                                + taskIdentify + "] failed.");
                    }
                }
                // one data block will be send to multiple server, mark successful if any try success
                if (sendSuccessful) {
                    putBlockId(taskToSuccessBlockIds, taskIdentify, sbi.getBlockId());
                } else {
                    putBlockId(taskToFailedBlockIds, taskIdentify, sbi.getBlockId());
                }
            }
        }

        private void putBlockId(Map<String, Set<Long>> taskToBlockIds, String taskAttempId, long blockId) {
            if (taskToBlockIds.get(taskAttempId) == null) {
                taskToBlockIds.put(taskAttempId, Sets.newHashSet());
            }
            taskToBlockIds.get(taskAttempId).add(blockId);
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
        this.taskToFailedBlockIds = Maps.newHashMap();
        this.taskToSuccessBlockIds = Maps.newHashMap();
        appId = SparkContext.getOrCreate().applicationId();
        String executorId = SparkEnv.get().executorId();
        if (!sparkConf.getBoolean(RssClientConfig.RSS_TEST_FLAG, false)
                && !SparkContext$.MODULE$.DRIVER_IDENTIFIER().equals(executorId)
                && !SparkContext$.MODULE$.LEGACY_DRIVER_IDENTIFIER().equals(executorId)) {
            // for non-driver executor, start a thread for sending shuffle data to shuffle server
            eventLoop.start();
        }
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

        // get all register info according to coordinator's response
        List<ShuffleRegisterInfo> shuffleRegisterInfos = ClientUtils.getShuffleRegisterInfos(response);
        // get ShuffleServer client and do register
        registerShuffleServers(appId, shuffleId, shuffleRegisterInfos);

        // get ShuffleServerHandler which will be used in writer and reader
        Map<Integer, List<ShuffleServerInfo>> partitionToServers =
                ClientUtils.getPartitionToServers(response);

        coordinatorClient.close();

        return new RssShuffleHandle(shuffleId, appId, numMaps, dependency, partitionToServers);
    }

    @VisibleForTesting
    protected void registerShuffleServers(String appId, int shuffleId,
            List<ShuffleRegisterInfo> shuffleRegisterInfos) {
        if (shuffleRegisterInfos == null || shuffleRegisterInfos.isEmpty()) {
            return;
        }
        shuffleRegisterInfos.parallelStream().forEach(registerInfo -> {
            ShuffleServerGrpcClient client =
                    ShuffleServerClientManager.getInstance().getClient(registerInfo.getShuffleServerInfo());
            client.registerShuffle(appId, shuffleId, registerInfo.getStart(), registerInfo.getEnd());
        });
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
            String taskIdentify = "" + context.taskAttemptId() + "_" + context.attemptNumber();
            long sendCheckTimeout = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT,
                    RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT_DEFAULT_VALUE);
            long sendCheckInterval = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL,
                    RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL_DEFAULT_VALUE);

            return new RssShuffleWriter(appId, rssHandle.getShuffleId(),
                    Integer.parseInt(SparkEnv.get().executorId()),
                    taskIdentify, context.taskMetrics().shuffleWriteMetrics(),
                    new BufferManagerOptions(sparkConf),
                    rssHandle, this, sendCheckTimeout, sendCheckInterval);
        } else {
            throw new RuntimeException("Unexpected ShuffleHandle:" + handle.getClass().getName());
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
            throw new RuntimeException("Unexpected ShuffleHandle:" + handle.getClass().getName());
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

    // this should be called in ExecutorPlugin.shutdown() to close all rpc clients
    public void closeClients() {
        ShuffleServerClientManager.getInstance().closeClients();
    }

    public EventLoop getEventLoop() {
        return eventLoop;
    }

    protected void setEventLoop(EventLoop<AddBlockEvent> eventLoop) {
        this.eventLoop = eventLoop;
    }

    public Set<Long> getFailedBlockIds(String taskIdentify) {
        Set<Long> result = taskToFailedBlockIds.get(taskIdentify);
        if (result == null) {
            result = Sets.newHashSet();
        }
        return result;
    }

    public Set<Long> getSuccessBlockIds(String taskIdentify) {
        Set<Long> result = taskToSuccessBlockIds.get(taskIdentify);
        if (result == null) {
            result = Sets.newHashSet();
        }
        return result;
    }

    @VisibleForTesting
    protected void addFailedBlockIds(String taskIdentify, Set<Long> blockIds) {
        if (taskToFailedBlockIds.get(taskIdentify) == null) {
            taskToFailedBlockIds.put(taskIdentify, Sets.newHashSet());
        }
        taskToFailedBlockIds.get(taskIdentify).addAll(blockIds);
    }

    @VisibleForTesting
    protected void addSuccessBlockIds(String taskIdentify, Set<Long> blockIds) {
        if (taskToSuccessBlockIds.get(taskIdentify) == null) {
            taskToSuccessBlockIds.put(taskIdentify, Sets.newHashSet());
        }
        taskToSuccessBlockIds.get(taskIdentify).addAll(blockIds);
    }

    @VisibleForTesting
    protected void clearCachedBlockIds() {
        taskToSuccessBlockIds.clear();
        taskToFailedBlockIds.clear();
    }
}
