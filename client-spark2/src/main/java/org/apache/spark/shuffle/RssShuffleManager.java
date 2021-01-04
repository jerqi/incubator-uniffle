package org.apache.spark.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tecent.rss.client.ClientUtils;
import com.tecent.rss.client.ShuffleServerClientManager;
import com.tencent.rss.common.CoordinatorGrpcClient;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerGrpcClient;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.proto.RssProtos;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
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
  private static final Logger LOG_RSS_INFO = LoggerFactory.getLogger(Constants.LOG4J_RSS_SHUFFLE_PREFIX);
  private SparkConf sparkConf;
  private String appId;
  private boolean isDriver;
  private Map<String, Set<Long>> taskToFailedBlockIds;
  private Map<String, Set<Long>> taskToSuccessBlockIds;

  private EventLoop eventLoop = new EventLoop<AddBlockEvent>("ShuffleDataQueue") {

    @Override
    public void onReceive(AddBlockEvent event) {
      List<ShuffleBlockInfo> shuffleBlockInfos = event.getShuffleDataInfo();
      String taskIdentify = event.getTaskIdentify();
      Map<ShuffleServerInfo, Map<Integer, Map<Integer, List<ShuffleBlockInfo>>>> serverToBlocks = Maps.newHashMap();
      Map<ShuffleServerInfo, List<Long>> serverToBlockIds = Maps.newHashMap();
      // send shuffle block to shuffle server
      // for all ShuffleBlockInfo, create the data structure as shuffleServer -> shuffleId -> partitionId -> blocks
      // it will be helpful to send rpc request to shuffleServer
      for (ShuffleBlockInfo sbi : shuffleBlockInfos) {
        int partitionId = sbi.getPartitionId();
        int shuffleId = sbi.getShuffleId();
        for (ShuffleServerInfo ssi : sbi.getShuffleServerInfos()) {
          if (!serverToBlockIds.containsKey(ssi)) {
            serverToBlockIds.put(ssi, Lists.newArrayList());
          }
          serverToBlockIds.get(ssi).add(sbi.getBlockId());

          if (!serverToBlocks.containsKey(ssi)) {
            serverToBlocks.put(ssi, Maps.newHashMap());
          }
          Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = serverToBlocks.get(ssi);
          if (!shuffleIdToBlocks.containsKey(shuffleId)) {
            shuffleIdToBlocks.put(shuffleId, Maps.newHashMap());
          }

          Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = shuffleIdToBlocks.get(shuffleId);
          if (!partitionToBlocks.containsKey(partitionId)) {
            partitionToBlocks.put(partitionId, Lists.newArrayList());
          }
          partitionToBlocks.get(partitionId).add(sbi);
        }
      }

      List<Long> tempFailedBlockIds = Lists.newArrayList();
      for (Map.Entry<ShuffleServerInfo, Map<Integer,
          Map<Integer, List<ShuffleBlockInfo>>>> entry : serverToBlocks.entrySet()) {
        ShuffleServerInfo ssi = entry.getKey();
        try {
          boolean sendSuccessful = ShuffleServerClientManager.getInstance()
              .getClient(ssi).sendShuffleDatas(appId, entry.getValue());
          if (sendSuccessful) {
            putBlockId(taskToSuccessBlockIds, taskIdentify, serverToBlockIds.get(ssi));
            LOG.info("Send: " + serverToBlockIds.get(ssi)
                + " to [" + ssi.getId() + "] successfully");
          } else {
            tempFailedBlockIds.addAll(serverToBlockIds.get(ssi));
            LOG.error("Send: " + serverToBlockIds.get(ssi) + " to [" + ssi.getId() + "] temp failed.");
          }
        } catch (Exception e) {
          tempFailedBlockIds.addAll(serverToBlockIds.get(ssi));
          LOG.error("Send: " + serverToBlockIds.get(ssi) + " to [" + ssi.getId() + "] temp failed.", e);
        }
      }
      if (!taskToSuccessBlockIds.get(taskIdentify).containsAll(tempFailedBlockIds)) {
        putBlockId(taskToFailedBlockIds, taskIdentify, tempFailedBlockIds);
        LOG.error("Send: " + tempFailedBlockIds + " failed.");
      }
    }

    private void putBlockId(Map<String, Set<Long>> taskToBlockIds, String taskAttempId, List<Long> blockIds) {
      if (taskToBlockIds.get(taskAttempId) == null) {
        taskToBlockIds.put(taskAttempId, Sets.newHashSet());
      }
      taskToBlockIds.get(taskAttempId).addAll(blockIds);
    }

    @Override
    public void onError(Throwable throwable) {
    }

    @Override
    public void onStart() {
    }
  };

  public RssShuffleManager(SparkConf sparkConf, boolean isDriver) {
    this.sparkConf = sparkConf;
    this.taskToFailedBlockIds = Maps.newHashMap();
    this.taskToSuccessBlockIds = Maps.newHashMap();
    this.isDriver = isDriver;
    if (!sparkConf.getBoolean(RssClientConfig.RSS_TEST_FLAG, false) || !isDriver) {
      // for non-driver executor, start a thread for sending shuffle data to shuffle server
      LOG.info("RSS data send thread is starting");
      eventLoop.start();
    }
  }

  @VisibleForTesting
  protected void setAppId() {
    appId = SparkContext.getOrCreate(sparkConf).applicationId();
  }

  // This method is called in Spark driver side,
  // and Spark driver will make some decision according to coordinator,
  // e.g. determining what RSS servers to use.
  // Then Spark driver will return a ShuffleHandle and
  // pass that ShuffleHandle to executors (getWriter/getReader).
  @Override
  public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {
    // SparkContext is created after RssShuffleManager, can't get appId in RssShuffleManager's construct
    setAppId();

    CoordinatorGrpcClient coordinatorClient = getCoordinatorClient();
    // ask coordinator for ShuffleServerHandler
    int partitionsPerServer = sparkConf.getInt(RssClientConfig.RSS_PARTITIONS_PER_SERVER,
        RssClientConfig.RSS_PARTITIONS_PER_SERVER_DEFAULT_VALUE);

    RssProtos.GetShuffleAssignmentsResponse response = coordinatorClient
        .getShuffleAssignments(appId, shuffleId, dependency.partitioner().numPartitions(), partitionsPerServer);

    // get all register info according to coordinator's response
    List<ShuffleRegisterInfo> shuffleRegisterInfos = ClientUtils.getShuffleRegisterInfos(response);
    // get ShuffleServer client and do register
    registerShuffleServers(appId, shuffleId, shuffleRegisterInfos);

    // get ShuffleServerHandler which will be used in writer and reader
    Map<Integer, List<ShuffleServerInfo>> partitionToServers =
        ClientUtils.getPartitionToServers(response);

    coordinatorClient.close();

    LOG_RSS_INFO.info("RegisterShuffle with ShuffleId[" + shuffleId + "], size:" + partitionToServers.size());
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
      for (ShuffleServerInfo ssi : entry.getValue()) {
        LOG_RSS_INFO.info("RegisterShuffle ShuffleId[" + shuffleId + "], partitionId[" + entry.getKey()
            + "], shuffleServer[" + ssi.getId() + "]");
      }
    }

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
      LOG_RSS_INFO.info("Send " + registerInfo + " successfully");
    });
  }

  @VisibleForTesting
  protected CoordinatorGrpcClient getCoordinatorClient() {
    LOG_RSS_INFO.info("Find coordinator[" + sparkConf.get(RssClientConfig.RSS_COORDINATOR_IP)
        + ":" + sparkConf.getInt(RssClientConfig.RSS_COORDINATOR_PORT,
        RssClientConfig.RSS_COORDINATOR_PORT_DEFAULT_VALUE) + "]");
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
      // SparkContext is created after RssShuffleManager, can't get appId in RssShuffleManager's construct
      setAppId();
      RssShuffleHandle rssHandle = (RssShuffleHandle) handle;
      String taskIdentify = "" + context.taskAttemptId() + "_" + context.attemptNumber();
      long sendCheckTimeout = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT,
          RssClientConfig.RSS_WRITER_SEND_CHECK_TIMEOUT_DEFAULT_VALUE);
      long sendCheckInterval = sparkConf.getLong(RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL,
          RssClientConfig.RSS_WRITER_SEND_CHECK_INTERVAL_DEFAULT_VALUE);
      int executorId = Integer.MAX_VALUE;
      if (!isDriver) {
        executorId = Integer.parseInt(SparkEnv.get().executorId());
      }

      return new RssShuffleWriter(appId, rssHandle.getShuffleId(),
          executorId, taskIdentify, context.taskMetrics().shuffleWriteMetrics(),
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
      // SparkContext is created after RssShuffleManager, can't get appId in RssShuffleManager's construct
      setAppId();
      String shuffleDataBasePath = sparkConf.get(RssClientConfig.RSS_BASE_PATH);
      RssShuffleHandle rssShuffleHandle = (RssShuffleHandle) handle;
      int partitionsPerServer = sparkConf.getInt(RssClientConfig.RSS_PARTITIONS_PER_SERVER,
          RssClientConfig.RSS_PARTITIONS_PER_SERVER_DEFAULT_VALUE);
      String fullShufflePath = RssUtils.getFullShuffleDataPath(shuffleDataBasePath,
          getShuffleDataPath(rssShuffleHandle.getShuffleId(),
              startPartition, partitionsPerServer,
              rssShuffleHandle.getDependency().partitioner().numPartitions()));
      if (StringUtils.isEmpty(shuffleDataBasePath)) {
        throw new RuntimeException("Can't get shuffle base path");
      }
      return new RssShuffleReader<K, C>(startPartition, endPartition, context,
          rssShuffleHandle, 0, fullShufflePath,
          sparkConf.getInt(RssClientConfig.RSS_INDEX_READ_LIMIT,
              RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE),
          SparkContext.getOrCreate().hadoopConfiguration());
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

  private String getShuffleDataPath(int shuffleId, int partitionId, int partitionsPerServer, int partitionNum) {
    int prNum = partitionNum % partitionsPerServer == 0
        ? partitionNum / partitionsPerServer : partitionNum / partitionsPerServer + 1;
    for (int i = 0; i < prNum; i++) {
      int start = i * partitionsPerServer;
      int end = Math.min(partitionNum, (i + 1) * partitionsPerServer - 1);
      if (partitionId >= start && partitionId <= end) {
        return RssUtils.getShuffleDataPath(appId, String.valueOf(shuffleId), start, end);
      }
    }
    throw new RuntimeException("Can't generate ShuffleData Path for appId[" + appId + "], shuffleId["
        + shuffleId + "], partitionId[" + partitionId + "], partitionsPerServer[" + partitionsPerServer
        + "], partitionNum[" + partitionNum + "]");
  }
}
