package org.apache.spark.shuffle.writer;

import com.clearspring.analytics.util.Lists;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.tencent.rss.client.util.ClientUtils;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.RssShuffleUtils;
import org.apache.spark.util.SizeEstimator$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteBufferManager extends MemoryConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(WriteBufferManager.class);
  private double sampleGrowRate;
  private int bufferSize;
  private long spillSize;
  // allocated bytes from executor memory
  private AtomicLong allocatedBytes = new AtomicLong(0);
  // bytes of shuffle data in memory
  private AtomicLong usedBytes = new AtomicLong(0);
  // bytes of shuffle data which is in send list
  private AtomicLong inSendListBytes = new AtomicLong(0);
  private long askExecutorMemory;
  private int executorId;
  private int shuffleId;
  private SerializerInstance instance;
  private ShuffleWriteMetrics shuffleWriteMetrics;
  // cache partition -> records
  private Map<Integer, WriterBuffer> buffers;
  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private int serializerBufferSize;
  private int serializerMaxBufferSize;
  private long copyTime = 0;
  private long serializeTime = 0;
  private long compressTime = 0;
  private long writeTime = 0;
  private long estimateTime = 0;
  private long requireMemoryTime = 0;
  private SerializationStream serializeStream;
  private Output output;
  private long recordNum = 0;
  private long nextSampleNum = 0;
  private long bytesPerRec = 0;
  private long sampleSize = 0;
  private long sampleNum = 0;

  public WriteBufferManager(
      int shuffleId,
      int executorId,
      BufferManagerOptions bufferManagerOptions,
      Serializer serializer,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      TaskMemoryManager taskMemoryManager,
      ShuffleWriteMetrics shuffleWriteMetrics) {
    super(taskMemoryManager);
    this.bufferSize = bufferManagerOptions.getBufferSize();
    this.spillSize = bufferManagerOptions.getBufferSpillThreshold();
    this.executorId = executorId;
    this.instance = serializer.newInstance();
    this.buffers = Maps.newHashMap();
    this.shuffleId = shuffleId;
    this.partitionToServers = partitionToServers;
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.serializerBufferSize = bufferManagerOptions.getSerializerBufferSize();
    this.serializerMaxBufferSize = bufferManagerOptions.getSerializerBufferMax();
    this.askExecutorMemory = bufferManagerOptions.getPreAllocatedBufferSize();
    this.sampleGrowRate = bufferManagerOptions.getSampleGrowRate();
    this.output = new Output(serializerBufferSize, serializerMaxBufferSize);
    this.serializeStream = instance.serializeStream(output);
  }

  // add record to cache, return [partition, ShuffleBlock] if meet spill condition
  public List<ShuffleBlockInfo> addRecord(int partitionId, Object key, Object value) {
    final long start = System.currentTimeMillis();
    recordNum++;
    if (recordNum >= nextSampleNum) {
      updateSamples(key, value);
    }
    List<ShuffleBlockInfo> result = Lists.newArrayList();
    if (buffers.containsKey(partitionId)) {
      WriterBuffer wb = buffers.get(partitionId);
      requestMemory(bytesPerRec);
      wb.addRecord(key, value, bytesPerRec);
      int estimatedSize = wb.getEstimatedSize();
      if (estimatedSize > bufferSize) {
        result.add(createShuffleBlock(partitionId,
            wb.getData(serializeStream, output, serializerBufferSize), wb.getEstimatedSize()));
        copyTime += wb.getCopyTime();
        serializeTime += wb.getSerializeTime();
        buffers.remove(partitionId);
        LOG.debug("Single buffer is full for shuffleId[" + shuffleId
            + "] partition[" + partitionId + "] with " + estimatedSize + " bytes");
      }
    } else {
      requestMemory(bytesPerRec);
      WriterBuffer wb = new WriterBuffer();
      wb.addRecord(key, value, bytesPerRec);
      if (bytesPerRec > bufferSize) {
        result.add(createShuffleBlock(partitionId,
            wb.getData(serializeStream, output, serializerBufferSize), wb.getEstimatedSize()));
        copyTime += wb.getCopyTime();
        serializeTime += wb.getSerializeTime();
        LOG.debug("Single buffer is full for shuffleId[" + shuffleId
            + "] partition[" + partitionId + "] with " + bytesPerRec + " bytes");
      } else {
        buffers.put(partitionId, wb);
      }

    }
    shuffleWriteMetrics.incRecordsWritten(1L);

    // check buffer size > spill threshold
    if (usedBytes.get() - inSendListBytes.get() > spillSize) {
      result = clear();
    }
    writeTime += System.currentTimeMillis() - start;
    return result;
  }

  public void updateSamples(Object key, Object value) {
    long start = System.currentTimeMillis();
    sampleSize += SizeEstimator$.MODULE$.estimate(key) + SizeEstimator$.MODULE$.estimate(value);
    estimateTime += System.currentTimeMillis() - start;
    sampleNum++;
    bytesPerRec = sampleSize / sampleNum;
    nextSampleNum = (long) Math.ceil(recordNum * sampleGrowRate);
  }

  // transform all [partition, records] to [partition, ShuffleBlockInfo] and clear cache
  public List<ShuffleBlockInfo> clear() {
    List<ShuffleBlockInfo> result = Lists.newArrayList();
    long dataSize = 0;
    for (Entry<Integer, WriterBuffer> entry : buffers.entrySet()) {
      WriterBuffer wb = entry.getValue();
      dataSize += wb.getEstimatedSize();
      if (wb.getEstimatedSize() > 0) {
        result.add(createShuffleBlock(entry.getKey(),
            wb.getData(serializeStream, output, serializerBufferSize), wb.getEstimatedSize()));
        copyTime += wb.getCopyTime();
        serializeTime += wb.getSerializeTime();
      }
    }
    LOG.info("Flush total buffer for shuffleId[" + shuffleId + "] with allocated["
        + allocatedBytes + "], dataSize[" + dataSize + "]");
    buffers.clear();
    return result;
  }

  // transform records to shuffleBlock
  private ShuffleBlockInfo createShuffleBlock(int partitionId, byte[] data, int freeMemory) {
    final int uncompressLength = data.length;
    long start = System.currentTimeMillis();
    final byte[] compressed = RssShuffleUtils.compressData(data);
    final long crc32 = ChecksumUtils.getCrc32(compressed);
    compressTime += System.currentTimeMillis() - start;
    long blockId = ClientUtils.getBlockId(executorId, ClientUtils.getAtomicInteger());
    shuffleWriteMetrics.incBytesWritten(compressed.length);
    // add memory to indicate bytes which will be sent to shuffle server
    inSendListBytes.addAndGet(freeMemory);
    return new ShuffleBlockInfo(shuffleId, partitionId, blockId, compressed.length, crc32,
        compressed, partitionToServers.get(partitionId), uncompressLength, freeMemory);
  }

  private void requestMemory(long requiredMem) {
    final long start = System.currentTimeMillis();
    if (allocatedBytes.get() - usedBytes.get() < requiredMem) {
      requestExecutorMemory(requiredMem);
    }
    usedBytes.addAndGet(requiredMem);
    requireMemoryTime += System.currentTimeMillis() - start;
  }

  private void requestExecutorMemory(long leastMem) {
    long gotMem = acquireMemory(askExecutorMemory);
    allocatedBytes.addAndGet(gotMem);
    int retry = 0;
    int maxRetry = 10;
    while (allocatedBytes.get() - usedBytes.get() < leastMem) {
      LOG.info("Can't get memory for now, sleep and try[" + retry
          + "] again, request[" + askExecutorMemory + "], got[" + gotMem + "] less than " + leastMem);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        LOG.warn("Exception happened when waiting for memory.", ie);
      }
      gotMem = acquireMemory(askExecutorMemory);
      allocatedBytes.addAndGet(gotMem);
      retry++;
      if (retry > maxRetry) {
        String message = "Can't get memory to cache shuffle data, request[" + askExecutorMemory
            + "], got[" + gotMem + "]," + " WriteBufferManager allocated[" + allocatedBytes + "] task used[" + used
            + "], consider to optimize 'spark.executor.memory'," + " 'spark.rss.writer.buffer.spill.size'.";
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
  protected long getAllocatedBytes() {
    return allocatedBytes.get();
  }

  @VisibleForTesting
  protected long getUsedBytes() {
    return usedBytes.get();
  }

  @VisibleForTesting
  protected long getInSendListBytes() {
    return inSendListBytes.get();
  }

  public void freeAllocatedMemory(long freeMemory) {
    freeMemory(freeMemory);
    allocatedBytes.addAndGet(-freeMemory);
    usedBytes.addAndGet(-freeMemory);
    inSendListBytes.addAndGet(-freeMemory);
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

  @VisibleForTesting
  protected long getSampleNum() {
    return sampleNum;
  }

  public long getWriteTime() {
    return writeTime;
  }

  public String getManagerCostInfo() {
    return "WriteBufferManager cost copyTime[" + copyTime + "], writeTime[" + writeTime + "], serializeTime["
        + serializeTime + "], compressTime[" + compressTime + "], estimateTime["
        + estimateTime + "], requireMemoryTime[" + requireMemoryTime + "]";
  }
}
