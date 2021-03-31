package org.apache.spark.shuffle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleRegisterInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.deploy.SparkHadoopUtil$;
import org.apache.spark.executor.ShuffleWriteMetrics;
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
  private String clientType;
  private ShuffleWriteClient shuffleWriteClient;
  private Map<String, Set<Long>> taskToSuccessBlockIds = Maps.newConcurrentMap();
  private Map<String, Set<Long>> taskToFailedBlockIds = Maps.newConcurrentMap();
  private Map<String, WriteBufferManager> taskToBuffManager = Maps.newConcurrentMap();
  private boolean heartbeatStarted = false;
  private long heartbeatInterval = 0;
  private ThreadPoolExecutor threadPoolExecutor;
  private EventLoop eventLoop = new EventLoop<AddBlockEvent>("ShuffleDataQueue") {

    @Override
    public void onReceive(AddBlockEvent event) {
      threadPoolExecutor.execute(() -> sendShuffleData(event.getTaskId(), event.getShuffleDataInfoList()));
    }

    private void sendShuffleData(String taskId, List<ShuffleBlockInfo> shuffleDataInfoList) {
      try {
        SendShuffleDataResult result = shuffleWriteClient.sendShuffleData(appId, shuffleDataInfoList);
        putBlockId(taskToSuccessBlockIds, taskId, result.getSuccessBlockIds());
        putBlockId(taskToFailedBlockIds, taskId, result.getFailedBlockIds());
      } finally {
        // data is already send, release the memory to executor
        long releaseSize = 0;
        for (ShuffleBlockInfo sbi : shuffleDataInfoList) {
          releaseSize += sbi.getFreeMemory();
        }
        taskToBuffManager.get(taskId).freeAllocatedMemory(releaseSize);
        LOG.debug("Finish send data and release " + releaseSize + " bytes");
      }
    }

    private synchronized void putBlockId(
        Map<String, Set<Long>> taskToBlockIds,
        String taskAttempId,
        Set<Long> blockIds) {
      if (blockIds == null) {
        return;
      }
      if (taskToBlockIds.get(taskAttempId) == null) {
        taskToBlockIds.put(taskAttempId, Sets.newConcurrentHashSet());
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
    this.heartbeatInterval = sparkConf.getLong(RssClientConfig.RSS_HEARTBEAT_INTERVAL,
        RssClientConfig.RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
    int retryMax = sparkConf.getInt(RssClientConfig.RSS_CLIENT_RETRY_MAX,
        RssClientConfig.RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryInterval = sparkConf.getLong(RssClientConfig.RSS_CLIENT_RETRY_INTERVAL,
        RssClientConfig.RSS_CLIENT_RETRY_INTERVAL_DEFAULT_VALUE);
    shuffleWriteClient =
        ShuffleClientFactory.getINSTANCE().createShuffleWriteClient(clientType, retryMax, retryInterval);
    registerCoordinator();
    if (!sparkConf.getBoolean(RssClientConfig.RSS_TEST_FLAG, false)) {
      // for non-driver executor, start a thread for sending shuffle data to shuffle server
      LOG.info("RSS data send thread is starting");
      eventLoop.start();
      int poolSize = sparkConf.getInt(RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE,
          RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_SIZE_DEFAULT_VALUE);
      int keepAliveTime = sparkConf.getInt(RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE,
          RssClientConfig.RSS_CLIENT_SEND_THREAD_POOL_KEEPALIVE_DEFAULT_VALUE);
      threadPoolExecutor = new ThreadPoolExecutor(poolSize, poolSize * 2, keepAliveTime, TimeUnit.SECONDS,
          Queues.newLinkedBlockingQueue(Integer.MAX_VALUE));
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
    startHeartbeat();

    int partitionNumPerRange = sparkConf.getInt(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE,
        RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE);
    int dataReplica = sparkConf.getInt(RssClientConfig.RSS_DATA_REPLICA,
        RssClientConfig.RSS_DATA_REPLICA_DEFAULT_VALUE);
    // get all register info according to coordinator's response
    ShuffleAssignmentsInfo response = shuffleWriteClient.getShuffleAssignments(
        appId, shuffleId, dependency.partitioner().numPartitions(), partitionNumPerRange, dataReplica);
    List<ShuffleRegisterInfo> shuffleRegisterInfoList = response.getRegisterInfoList();
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = response.getPartitionToServers();
    Set<ShuffleServerInfo> shuffleServerForResult = response.getShuffleServersForResult();

    registerShuffleServers(appId, shuffleId, shuffleRegisterInfoList);

    LOG.debug("RegisterShuffle with ShuffleId[" + shuffleId + "], size:" + partitionToServers.size());
    LOG.debug("Shuffle result assignment with ShuffleId[" + shuffleId + "], " + shuffleServerForResult);
    for (Map.Entry<Integer, List<ShuffleServerInfo>> entry : partitionToServers.entrySet()) {
      for (ShuffleServerInfo ssi : entry.getValue()) {
        LOG.debug("RegisterShuffle ShuffleId[" + shuffleId + "], partitionId[" + entry.getKey()
            + "], shuffleServer[" + ssi.getId() + "]");
      }
    }

    return new RssShuffleHandle(shuffleId, appId, numMaps, dependency, partitionToServers, shuffleServerForResult);
  }

  private void startHeartbeat() {
    if (!sparkConf.getBoolean(RssClientConfig.RSS_TEST_FLAG, false) && !heartbeatStarted) {
      new Thread(() -> {
        try {
          while (true) {
            shuffleWriteClient.sendAppHeartbeat(appId);
            Thread.sleep(heartbeatInterval);
          }
        } catch (Exception e) {
          LOG.warn("Error happened when keep heartbeat from application to coordinator", e);
        }
      }).start();
      heartbeatStarted = true;
    }
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
      LOG.debug("Register with " + sri + " successfully");
    }
  }

  @VisibleForTesting
  protected void registerCoordinator() {
    String host = sparkConf.get(RssClientConfig.RSS_COORDINATOR_IP);
    int port = Integer.parseInt(sparkConf.get(RssClientConfig.RSS_COORDINATOR_PORT));
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
      ShuffleWriteMetrics writeMetrics = context.taskMetrics().shuffleWriteMetrics();
      WriteBufferManager bufferManager = new WriteBufferManager(
          shuffleId, executorId, bufferOptions, rssHandle.getDependency().serializer(),
          rssHandle.getPartitionToServers(), context.taskMemoryManager(),
          writeMetrics);
      taskToBuffManager.put(taskId, bufferManager);

      return new RssShuffleWriter(appId, shuffleId, taskId, bufferManager,
          writeMetrics, this, sparkConf, shuffleWriteClient, rssHandle);
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
      // spark.rss.base.path is not necessary for every storage type, eg, hdfs need but localfile doesn't
      String shuffleDataBasePath = sparkConf.get(RssClientConfig.RSS_BASE_PATH, "");
      String storageType = sparkConf.get(RssClientConfig.RSS_STORAGE_TYPE);
      int indexReadLimit = sparkConf.getInt(RssClientConfig.RSS_INDEX_READ_LIMIT,
          RssClientConfig.RSS_INDEX_READ_LIMIT_DEFAULT_VALUE);
      RssShuffleHandle rssShuffleHandle = (RssShuffleHandle) handle;
      int partitionNumPerRange = sparkConf.getInt(RssClientConfig.RSS_PARTITION_NUM_PER_RANGE,
          RssClientConfig.RSS_PARTITION_NUM_PER_RANGE_DEFAULT_VALUE);
      int partitionNum = rssShuffleHandle.getDependency().partitioner().numPartitions();
      long readBufferSize = sparkConf.getSizeAsBytes(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE,
          RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE_DEFAULT_VALUE);
      if (readBufferSize > Integer.MAX_VALUE) {
        LOG.warn(RssClientConfig.RSS_CLIENT_READ_BUFFER_SIZE + " can support 2g as max");
        readBufferSize = Integer.MAX_VALUE;
      }
      List<Long> expectedBlockIds = shuffleWriteClient.getShuffleResult(
          clientType, rssShuffleHandle.getShuffleServersForResult(),
          appId, rssShuffleHandle.getShuffleId(), startPartition);

      return new RssShuffleReader<K, C>(startPartition, endPartition, context,
          rssShuffleHandle, shuffleDataBasePath, indexReadLimit,
          SparkHadoopUtil$.MODULE$.newConfiguration(SparkEnv.get().conf()),
          storageType, (int) readBufferSize, partitionNumPerRange, partitionNum,
          Sets.newHashSet(expectedBlockIds));
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
