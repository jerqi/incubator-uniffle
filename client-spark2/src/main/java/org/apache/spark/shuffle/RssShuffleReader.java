package org.apache.spark.shuffle;

import avro.shaded.com.google.common.collect.Sets;
import com.tecent.rss.client.ClientUtils;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.reader.RssShuffleDataIterator;
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

    private String appId;
    private int shuffleId;
    private int startPartition;
    private int endPartition;
    private TaskContext context;
    private ShuffleDependency<K, C, ?> shuffleDependency;
    private int timeoutMillis;
    private Serializer serializer;
    private String taskIdentify;
    private Configuration hadoopConf;
    private String basePath;
    private int indexReadLimit;

    public RssShuffleReader(
            int startPartition,
            int endPartition,
            TaskContext context,
            RssShuffleHandle rssShuffleHandle,
            int timeoutMillis,
            String basePath,
            int indexReadLimit,
            Configuration hadoopConf) {
        this.appId = rssShuffleHandle.getAppId();
        this.shuffleId = rssShuffleHandle.getShuffleId();
        this.startPartition = startPartition;
        this.endPartition = endPartition;
        this.context = context;
        this.hadoopConf = hadoopConf;
        this.shuffleDependency = rssShuffleHandle.getDependency();
        this.timeoutMillis = timeoutMillis;
        this.serializer = rssShuffleHandle.getDependency().serializer();
        this.taskIdentify = "" + context.taskAttemptId() + "_" + context.attemptNumber();
        this.basePath = basePath;
        this.indexReadLimit = indexReadLimit;
    }

    @Override
    public Iterator<Product2<K, C>> read() {
        LOG.info("Shuffle read started:" + getReadInfo());
        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator<K, C>(
                indexReadLimit, shuffleDependency.serializer(), getExpectedBlockIds());
        rssShuffleDataIterator.init(basePath, hadoopConf);

        Iterator<Product2<K, C>> resultIter = null;
        Iterator<Product2<K, C>> aggregatedIter = null;

        if (shuffleDependency.mapSideCombine()) {
            // doesn't support for now
            throw new RuntimeException("MapSideCombine is not supported in Rss");
        }

        if (shuffleDependency.aggregator().isDefined()) {
            // We don't know the value type, but also don't care -- the dependency *should*
            // have made sure its compatible w/ this aggregator, which will convert the value
            // type to the combined type C
            aggregatedIter = shuffleDependency.aggregator().get().combineCombinersByKey(
                    rssShuffleDataIterator, context);
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
        while (mapStatusIter.hasNext()) {
            Tuple2<BlockManagerId, Seq<Tuple2<BlockId, Object>>> tuple2 = mapStatusIter.next();
            Option<String> topologyInfo = tuple2._1().topologyInfo();
            if (topologyInfo.isDefined()) {
                String jsonStr = tuple2._1().topologyInfo().get();
                try {
                    Map<Integer, Set<Long>> partitionToBlockIds = ClientUtils.getBlockIdsFromJson(jsonStr);
                    if (partitionToBlockIds.containsKey(startPartition)) {
                        expectedBlockIds.addAll(partitionToBlockIds.get(startPartition));
                    }
                } catch (IOException ioe) {
                    LOG.warn("Error happened when parse topologyInfo for partitionsToBlockIds:" + jsonStr, ioe);
                }
            } else {
                LOG.warn("Can't get partitionsToBlockIds for " + getReadInfo());
            }
        }
        LOG.info("Got result from mapOutputTracker.getMapSizesByExecutorId for expected blockIds");
        return expectedBlockIds;
    }

    private String getReadInfo() {
        return "appId=" + appId
                + ", shuffleId=" + shuffleId
                + ",taskIdentify=" + taskIdentify
                + ", partitions: [" + startPartition
                + ", " + endPartition + ")";
    }
}
