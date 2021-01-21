package org.apache.spark.shuffle.writer;

import com.clearspring.analytics.util.Lists;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.tecent.rss.client.util.ClientUtils;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;

public class WriteBufferManager extends MemoryConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(WriteBufferManager.class);

  private int bufferSize;
  private int maxBufferSize;
  private long spillSize;
  private long totalBytes;
  private int executorId;
  private int shuffleId;
  private SerializerInstance instance;
  private ShuffleWriteMetrics shuffleWriteMetrics;
  // cache partition -> records
  private Map<Integer, WriterBuffer> buffers;
  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

  public WriteBufferManager(
      int shuffleId,
      int executorId,
      BufferManagerOptions bufferManagerOptions,
      Serializer serializer,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      TaskMemoryManager taskMemoryManager,
      ShuffleWriteMetrics shuffleWriteMetrics) {
    super(taskMemoryManager);
    this.bufferSize = bufferManagerOptions.getIndividualBufferSize();
    this.maxBufferSize = bufferManagerOptions.getIndividualBufferMax();
    this.spillSize = bufferManagerOptions.getBufferSpillThreshold();
    this.executorId = executorId;
    this.instance = serializer.newInstance();
    this.buffers = Maps.newHashMap();
    this.totalBytes = 0L;
    this.shuffleId = shuffleId;
    this.partitionToServers = partitionToServers;
    this.shuffleWriteMetrics = shuffleWriteMetrics;
  }

  // add record to cache, return [partition, ShuffleBlock] if meet spill condition
  public List<ShuffleBlockInfo> addRecord(int partitionId, Object key, Object value) {
    List<ShuffleBlockInfo> result = Lists.newArrayList();
    if (buffers.containsKey(partitionId)) {
      SerializationStream serializeStream = buffers.get(partitionId).getSerializeStream();
      Output output = buffers.get(partitionId).getOutput();
      int oldSize = output.position();
      serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
      serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
      serializeStream.flush();
      int newSize = output.position();
      int kvSize = newSize - oldSize;
      updateWriteMetrics(1L, kvSize);
      if (newSize >= bufferSize) {
        result.add(createShuffleBlock(partitionId, output.toBytes()));
        serializeStream.close();
        buffers.remove(partitionId);
        totalBytes -= oldSize;
      } else {
        totalBytes += kvSize;
      }
    } else {
      // Output will cost maxBufferSize memory at most, ask TaskMemoryManager for it
      requestMemory(maxBufferSize);
      Output output = new Output(bufferSize, maxBufferSize);
      SerializationStream serializeStream = instance.serializeStream(output);
      serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
      serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
      serializeStream.flush();
      int newSize = output.position();
      updateWriteMetrics(1L, newSize);
      if (newSize >= bufferSize) {
        result.add(createShuffleBlock(partitionId, output.toBytes()));
        serializeStream.close();
      } else {
        buffers.put(partitionId, new WriterBuffer(serializeStream, output));
        totalBytes = totalBytes + newSize;
      }
    }

    // check buffer size > spill threshold
    if (totalBytes >= spillSize) {
      for (Entry<Integer, WriterBuffer> entry : buffers.entrySet()) {
        result.add(createShuffleBlock(entry.getKey(), entry.getValue().getOutput().toBytes()));
        entry.getValue().getSerializeStream().close();
      }
      buffers.clear();
      totalBytes = 0;
    }

    return result;
  }

  // transform all [partition, records] to [partition, ShuffleBlockInfo] and clear cache
  public List<ShuffleBlockInfo> clear() {
    List<ShuffleBlockInfo> result = Lists.newArrayList();
    for (Entry<Integer, WriterBuffer> entry : buffers.entrySet()) {
      result.add(createShuffleBlock(entry.getKey(), entry.getValue().getOutput().toBytes()));
      entry.getValue().getSerializeStream().close();
    }
    buffers.clear();
    totalBytes = 0;
    return result;
  }

  // transform records to shuffleBlock
  private ShuffleBlockInfo createShuffleBlock(int partitionId, byte[] data) {
    long crc32 = ChecksumUtils.getCrc32(data);
    return new ShuffleBlockInfo(shuffleId, partitionId,
        ClientUtils.getBlockId(executorId, ClientUtils.getAtomicInteger()),
        data.length, crc32, data, partitionToServers.get(partitionId));
  }

  private void updateWriteMetrics(long recordNum, long size) {
    shuffleWriteMetrics.incRecordsWritten(recordNum);
    shuffleWriteMetrics.incBytesWritten(size);
  }

  private void requestMemory(long requiredMem) {
    long gotMem = acquireMemory(requiredMem);
    int retry = 0;
    int maxRetry = 10;
    while (gotMem < requiredMem) {
      LOG.info("Can't get memory for now, sleep and try[" + retry
          + "] again, request[" + requiredMem + "], got[" + gotMem + "]");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        LOG.warn("Exception happened when waiting for memory.", ie);
      }
      gotMem = acquireMemory(requiredMem);
      retry++;
      if (retry > maxRetry) {
        String message = "Can't get memory to cache shuffle data, request[" + requiredMem + "], got[" + gotMem + "],"
            + " executor used[" + used + "], consider to optimize 'spark.executor.memory',"
            + " 'spark.rss.writer.buffer.spill.size', 'spark.rss.writer.buffer.max.size'.";
        LOG.error(message);
        throw new OutOfMemoryError(message);
      }
    }
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) {
    // there is no spill for such situation
    return 0;
  }

  @VisibleForTesting
  protected long getTotalBytes() {
    return totalBytes;
  }

  @VisibleForTesting
  protected Map<Integer, WriterBuffer> getBuffers() {
    return buffers;
  }

  @VisibleForTesting
  protected ShuffleWriteMetrics getShuffleWriteMetrics() {
    return shuffleWriteMetrics;
  }

  @VisibleForTesting
  protected void setShuffleWriteMetrics(ShuffleWriteMetrics shuffleWriteMetrics) {
    this.shuffleWriteMetrics = shuffleWriteMetrics;
  }
}
