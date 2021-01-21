package org.apache.spark.shuffle.reader;

import avro.shaded.com.google.common.collect.Sets;
import com.tecent.rss.client.ShuffleReadClient;
import com.tecent.rss.client.factory.ShuffleClientFactory;
import com.tecent.rss.client.request.CreateShuffleReadClientRequest;
import com.tecent.rss.client.util.ClientUtils;
import com.tencent.rss.common.util.Constants;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.util.CompletionIterator$;
import org.apache.spark.util.collection.ExternalSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class RssShuffleReader<K, C> implements ShuffleReader<K, C> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleReader.class);
  private static final Logger LOG_RSS_INFO = LoggerFactory.getLogger(Constants.LOG4J_RSS_SHUFFLE_PREFIX);

  private String appId;
  private int shuffleId;
  private int startPartition;
  private int endPartition;
  private TaskContext context;
  private ShuffleDependency<K, C, ?> shuffleDependency;
  private Serializer serializer;
  private String taskId;
  private Configuration hadoopConf;
  private String basePath;
  private int indexReadLimit;
  private String storageType;

  public RssShuffleReader(
      int startPartition,
      int endPartition,
      TaskContext context,
      RssShuffleHandle rssShuffleHandle,
      String basePath,
      int indexReadLimit,
      Configuration hadoopConf,
      String storageType) {
    this.appId = rssShuffleHandle.getAppId();
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.context = context;
    this.hadoopConf = hadoopConf;
    this.shuffleDependency = rssShuffleHandle.getDependency();
    this.shuffleId = shuffleDependency.shuffleId();
    this.serializer = rssShuffleHandle.getDependency().serializer();
    this.taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    this.basePath = basePath;
    this.indexReadLimit = indexReadLimit;
    this.storageType = storageType;
  }

  @Override
  public Iterator<Product2<K, C>> read() {
    LOG.info("Shuffle read started:" + getReadInfo());

    CreateShuffleReadClientRequest request = new CreateShuffleReadClientRequest(
        storageType, basePath, hadoopConf, indexReadLimit, getExpectedBlockIds());
    ShuffleReadClient shuffleReadClient = ShuffleClientFactory.getINSTANCE().createShuffleReadClient(request);

    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator<K, C>(
        shuffleDependency.serializer(), shuffleReadClient, context.taskMetrics().shuffleReadMetrics());
    rssShuffleDataIterator.checkExpectedBlockIds();

    Iterator<Product2<K, C>> resultIter = null;
    Iterator<Product2<K, C>> aggregatedIter = null;

    if (shuffleDependency.aggregator().isDefined()) {
      if (shuffleDependency.mapSideCombine()) {
        // We are reading values that are already combined
        aggregatedIter = shuffleDependency.aggregator().get().combineCombinersByKey(
            rssShuffleDataIterator, context);
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        aggregatedIter = shuffleDependency.aggregator().get().combineCombinersByKey(
            rssShuffleDataIterator, context);
      }
    } else {
      aggregatedIter = rssShuffleDataIterator;
    }

    if (shuffleDependency.keyOrdering().isDefined()) {
      // Create an ExternalSorter to sort the data
      ExternalSorter sorter = new ExternalSorter<K, C, C>(context, Option.empty(), Option.empty(),
          shuffleDependency.keyOrdering(), serializer);
      LOG.info("Inserting aggregated records to sorter");
      long startTime = System.currentTimeMillis();
      sorter.insertAll(aggregatedIter);
      LOG.info("Inserted aggregated records to sorter: millis:" + (System.currentTimeMillis() - startTime));
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled());
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled());
      context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes());
      // Use completion callback to stop sorter if task was finished/cancelled.
      Function1<TaskContext, Void> fn1 = new AbstractFunction1<TaskContext, Void>() {
        public Void apply(TaskContext context) {
          sorter.stop();
          return (Void) null;
        }
      };
      context.addTaskCompletionListener(fn1);
      Function0<BoxedUnit> fn0 = new AbstractFunction0<BoxedUnit>() {
        @Override
        public BoxedUnit apply() {
          sorter.stop();
          return BoxedUnit.UNIT;
        }
      };
      resultIter = CompletionIterator$.MODULE$.apply(sorter.iterator(), fn0);
    } else {
      resultIter = aggregatedIter;
    }

    if (!(resultIter instanceof InterruptibleIterator)) {
      resultIter = new InterruptibleIterator<>(context, resultIter);
    }
    return resultIter;
  }

  public Set<Long> getExpectedBlockIds() {
    Set<Long> expectedBlockIds = Sets.newHashSet();
    Iterator<Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>>> mapStatusIter =
        SparkEnv.get().mapOutputTracker().getMapSizesByExecutorId(
            shuffleId, startPartition, endPartition, false);
    LOG_RSS_INFO.info("Start getExpectedBlockIds for shuffleId["
        + shuffleId + "], partitionId[" + startPartition + "]");
    while (mapStatusIter.hasNext()) {
      Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>> tuple2 = mapStatusIter.next();
      Option<String> topologyInfo = tuple2._1().topologyInfo();
      if (topologyInfo.isDefined()) {
        String jsonStr = tuple2._1().topologyInfo().get();
        try {
          Map<Integer, Set<Long>> partitionToBlockIds = ClientUtils.getBlockIdsFromJson(jsonStr);
          for (Map.Entry<Integer, Set<Long>> entry : partitionToBlockIds.entrySet()) {
            StringBuilder sb = new StringBuilder();
            for (Long blockId : entry.getValue()) {
              sb.append(blockId).append(",");
            }
            LOG_RSS_INFO.info("Find BlockIds for shuffleId[" + shuffleId + "], partitionId["
                + entry.getKey() + "], blockIds[" + sb.toString() + "]");
          }
          if (partitionToBlockIds.containsKey(startPartition)) {
            expectedBlockIds.addAll(partitionToBlockIds.get(startPartition));
          }
        } catch (IOException ioe) {
          LOG.error("Error happened when parse topologyInfo for partitionsToBlockIds:" + jsonStr, ioe);
        }
      } else {
        LOG.error("Can't get partitionsToBlockIds for " + getReadInfo());
      }
    }
    StringBuilder sb = new StringBuilder();
    for (Long blockId : expectedBlockIds) {
      sb.append(blockId).append(",");
    }
    LOG_RSS_INFO.info("Finish getExpectedBlockIds for shuffleId[" + shuffleId
        + "], partitionId[" + startPartition + "], blockIds[" + sb.toString() + "]");
    return expectedBlockIds;
  }

  private String getReadInfo() {
    return "appId=" + appId
        + ", shuffleId=" + shuffleId
        + ",taskId=" + taskId
        + ", partitions: [" + startPartition
        + ", " + endPartition + ")";
  }

}
