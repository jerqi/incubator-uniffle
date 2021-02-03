package org.apache.spark.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.client.response.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.RssUtils;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.deploy.SparkHadoopUtil$;
import org.apache.spark.shuffle.reader.RssShuffleReader;
import org.apache.spark.shuffle.writer.AddBlockEvent;
import org.apache.spark.shuffle.writer.BufferManagerOptions;
import org.apache.spark.shuffle.writer.RssShuffleWriter;
import org.apache.spark.shuffle.writer.WriteBufferManager;
import org.apache.spark.util.EventLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RssShuffleManager implements ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleManager.class);
  private SparkConf sparkConf;
  private String appId;
  private boolean isDriver;
  private int serializerBufferSize = 0;
  private int serializerMaxBufferSize = 0;
  private String clientType;
  private ShuffleWriteClient shuffleWriteClient;
  private Map<String, Set<Long>> taskToSuccessBlockIds = Maps.newHashMap();
  private Map<String, Set<Long>> taskToFailedBlockIds = Maps.newHashMap();
  private Map<String, WriteBufferManager> taskToBuffManager = Maps.newHashMap();
  private EventLoop eventLoop = new EventLoop<AddBlockEvent>("ShuffleDataQueue") {

    @Override
    public void onReceive(AddBlockEvent event) {
      List<ShuffleBlockInfo> shuffleDataInfoList = event.getShuffleDataInfoList();
      String taskId = event.getTaskId();
      try {
        SendShuffleDataResult result = shuffleWriteClient.sendShuffleData(appId, shuffleDataInfoList);
        putBlockId(taskToSuccessBlockIds, taskId, result.getSuccessBlockIds());
        putBlockId(taskToFailedBlockIds, taskId, result.getFailedBlockIds());
      } finally {
        // data is already send, release the memory to executor
        long releaseSize = 0;
        for (ShuffleBlockInfo sbi : shuffleDataInfoList) {
          if (sbi.getLength() > serializerBufferSize) {
            releaseSize += sbi.getLength() + serializerMaxBufferSize;
          } else {
            releaseSize += serializerMaxBufferSize;
          }
        }
        taskToBuffManager.get(taskId).freeMemory(releaseSize);
        LOG.info("Finish send data and release " + releaseSize + " bytes");
      }
    }

    private void putBlockId(Map<String, Set<Long>> taskToBlockIds, String taskAttempId, List<Long> blockIds) {
      if (blockIds == null) {
        return;
      }
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
    this.isDriver = isDriver;
    this.clientType = sparkConf.get(RssClientConfig.RSS_CLIENT_TYPE,
        RssClientConfig.RSS_CLIENT_TYPE_DEFAULT_VALUE);
    int retryMax = sparkConf.getInt(RssClientConfig.RSS_CLIENT_RETRY_MAX,
        RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryInterval = sparkConf.getLong(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL,
        RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_DEFAULT_VALUE);
    shuffleWriteClient =
        ShuffleClientFactory.getINSTANCE().createShuffleWriteClient(clientType, retryMax, retryInterval);
    BufferManagerOptions bufferOptions = new BufferManagerOptions(sparkConf);
    serializerBufferSize = bufferOptions.getSerializerBufferSize();
    serializerMaxBufferSize = bufferOptions.getSerializerBufferMax();
    registerCoordinator();
    if (!sparkConf.getBoolean(RssClientConfig.RSS_TEST_FLAG, false) || !isDriver) {
      // for non-driver executor, start a thread for sending shuffle data to shuffle server
      LOG.info("RSS data send thread is starting");
      eventLoop.start();
    }
  }

  @VisibleForTesting
  protected void setAppId() {
    appId = SparkEnv.get().conf().getAppId();
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

    int partitionsPerServer = sparkConf.getInt(RssClientConfig.RSS_PARTITIONS_PER_SERVER,
        RssClientConfig.RSS_PARTITIONS_PER_SERVER_DEFAULT_VALUE);
    // get all register info according to coordinator's response
    ShuffleAssignmentsInfo response = shuffleWriteClient.getShuffleAssignments(
        appId, shuffleId, dependency.partitioner().numPartitions(), partitionsPerServer);
    List<ShuffleRegisterInfo> shuffleRegisterInfoList = response.getRegisterInfoList();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = response.getPartitionToServers();
    Set<ShuffleServerInfo> shuffleServerForResult = response.getShuffleServersForResult();

    registerShuffleServers(appId, shuffleId, shuffleRegisterInfoList);

    LOG.info("RegisterShuffle with ShuffleId[" + shuffleId + "], size:" + partitionToServers.size());
    LOG.info("Shuffle result assignment with ShuffleId[" + shuffleId + "], " + shuffleServerForResult);
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
      for (ShuffleServerInfo ssi : entry.getValue()) {
        LOG.info("RegisterShuffle ShuffleId[" + shuffleId + "], partitionId[" + entry.getKey()
            + "], shuffleServer[" + ssi.getId() + "]");
      }
    }

    return new RssShuffleHandle(shuffleId, appId, numMaps, dependency, partitionToServers, shuffleServerForResult);
  }

  @VisibleForTesting
  protected void registerShuffleServers(String appId, int shuffleId,
      List<ShuffleRegisterInfo> shuffleRegisterInfoList) {
    if (shuffleRegisterInfoList == null || shuffleRegisterInfoList.isEmpty()) {
      return;
    }
    for (ShuffleRegisterInfo sri : shuffleRegisterInfoList) {
      shuffleWriteClient.registerShuffle(
          sri.getShuffleServerInfo(), appId, shuffleId, sri.getStart(), sri.getEnd());
      LOG.info("Register with " + sri + " successfully");
    }
  }

  @VisibleForTesting
  protected void registerCoordinator() {
    String host = sparkConf.get(RssClientConfig.RSS_COORDINATOR_IP);
    int port = sparkConf.getInt(RssClientConfig.RSS_COORDINATOR_PORT,
        RssClientConfig.RSS_COORDINATOR_PORT_DEFAULT_VALUE);
    LOG.info("Registering coordinator client [" + host + ":" + port + "]");
    shuffleWriteClient.registerCoordinatorClient(host, port);
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
      int executorId = Integer.MAX_VALUE;
      if (!isDriver) {
        executorId = Integer.parseInt(SparkEnv.get().executorId());
      }

      int shuffleId = rssHandle.getShuffleId();
      String taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
      BufferManagerOptions bufferOptions = new BufferManagerOptions(sparkConf);
      WriteBufferManager bufferManager = new WriteBufferManager(
          shuffleId, executorId, bufferOptions, rssHandle.getDependency().serializer(),
          rssHandle.getPartitionToServers(), context.taskMemoryManager(),
          context.taskMetrics().shuffleWriteMetrics());
      taskToBuffManager.put(taskId, bufferManager);

      return new RssShuffleWriter(appId, shuffleId, taskId, bufferManager,
          context.taskMetrics().shuffleWriteMetrics(), rssHandle.getDependency(),
          this, sparkConf, shuffleWriteClient, rssHandle.getShuffleServersForResult());
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
      String storageType = sparkConf.get(RssClientConfig.RSS_STORAGE_TYPE,
          RssClientConfig.RSS_STORAGE_TYPE_DEFAULT_VALUE);
      int indexReadLimit = sparkConf.getInt(RssClientConfig.RSS_INDEX_READ_LIMIT,
          RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE);
      RssShuffleHandle rssShuffleHandle = (RssShuffleHandle) handle;
      int partitionsPerServer = sparkConf.getInt(RssClientConfig.RSS_PARTITIONS_PER_SERVER,
          RssClientConfig.RSS_PARTITIONS_PER_SERVER_DEFAULT_VALUE);
      String fullShufflePath = RssUtils.getFullShuffleDataFolder(shuffleDataBasePath,
          RssUtils.getShuffleDataPathWithRange(appId, rssShuffleHandle.getShuffleId(),
              startPartition, partitionsPerServer,
              rssShuffleHandle.getDependency().partitioner().numPartitions()));
      if (StringUtils.isEmpty(shuffleDataBasePath)) {
        throw new RuntimeException("Can't get shuffle base path");
      }

      List<Long> expectedBlockIds = shuffleWriteClient.getShuffleResult(
          clientType, rssShuffleHandle.getShuffleServersForResult(),
          appId, rssShuffleHandle.getShuffleId(), startPartition);

      return new RssShuffleReader<K, C>(startPartition, endPartition, context,
          rssShuffleHandle, fullShufflePath, indexReadLimit,
          SparkHadoopUtil$.MODULE$.newConfiguration(SparkEnv.get().conf()),
          storageType, Sets.newHashSet(expectedBlockIds));
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
    shuffleWriteClient.close();
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    throw new RuntimeException("RssShuffleManager.shuffleBlockResolver is not implemented");
  }

  public EventLoop getEventLoop() {
    return eventLoop;
  }

  @VisibleForTesting
  public void setEventLoop(EventLoop<AddBlockEvent> eventLoop) {
    this.eventLoop = eventLoop;
  }

  public Set<Long> getFailedBlockIds(String taskId) {
    Set<Long> result = taskToFailedBlockIds.get(taskId);
    if (result == null) {
      result = Sets.newHashSet();
    }
    return result;
  }

  public Set<Long> getSuccessBlockIds(String taskId) {
    Set<Long> result = taskToSuccessBlockIds.get(taskId);
    if (result == null) {
      result = Sets.newHashSet();
    }
    return result;
  }

  @VisibleForTesting
  public void addFailedBlockIds(String taskId, Set<Long> blockIds) {
    if (taskToFailedBlockIds.get(taskId) == null) {
      taskToFailedBlockIds.put(taskId, Sets.newHashSet());
    }
    taskToFailedBlockIds.get(taskId).addAll(blockIds);
  }

  @VisibleForTesting
  public void addSuccessBlockIds(String taskId, Set<Long> blockIds) {
    if (taskToSuccessBlockIds.get(taskId) == null) {
      taskToSuccessBlockIds.put(taskId, Sets.newHashSet());
    }
    taskToSuccessBlockIds.get(taskId).addAll(blockIds);
  }

  @VisibleForTesting
  public void clearCachedBlockIds() {
    taskToSuccessBlockIds.clear();
    taskToFailedBlockIds.clear();
  }
}
