package org.apache.spark.shuffle.reader;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.client.request.CreateShuffleReadClientRequest;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.util.CompletionIterator$;
import org.apache.spark.util.collection.ExternalSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public class RssShuffleReader<K, C> implements ShuffleReader<K, C> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleReader.class);

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
  private int readBufferSize;
  private int partitionsPerServer;
  private int partitionNum;
  private String storageType;
  private Set<Long> expectedBlockIds;
  private List<ShuffleServerInfo> shuffleServerInfoList;

  public RssShuffleReader(
      int startPartition,
      int endPartition,
      TaskContext context,
      RssShuffleHandle rssShuffleHandle,
      String basePath,
      int indexReadLimit,
      Configuration hadoopConf,
      String storageType,
      int readBufferSize,
      int partitionsPerServer,
      int partitionNum,
      Set<Long> expectedBlockIds) {
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
    this.readBufferSize = readBufferSize;
    this.partitionsPerServer = partitionsPerServer;
    this.partitionNum = partitionNum;
    this.expectedBlockIds = expectedBlockIds;
    this.shuffleServerInfoList =
        (List<ShuffleServerInfo>) (rssShuffleHandle.getPartitionToServers().get(startPartition));
  }

  @Override
  public Iterator<Product2<K, C>> read() {
    LOG.info("Shuffle read started:" + getReadInfo());

    CreateShuffleReadClientRequest request = new CreateShuffleReadClientRequest(
        appId, shuffleId, startPartition, storageType, basePath, hadoopConf, indexReadLimit,
        readBufferSize, partitionsPerServer, partitionNum, expectedBlockIds, shuffleServerInfoList);
    ShuffleReadClient shuffleReadClient = ShuffleClientFactory.getINSTANCE().createShuffleReadClient(request);

    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator<K, C>(
        shuffleDependency.serializer(), shuffleReadClient,
        context.taskMetrics().shuffleReadMetrics());

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

  private String getReadInfo() {
    return "appId=" + appId
        + ", shuffleId=" + shuffleId
        + ",taskId=" + taskId
        + ", partitions: [" + startPartition
        + ", " + endPartition + ")";
  }

}
