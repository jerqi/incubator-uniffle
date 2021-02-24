package org.apache.spark.shuffle.writer;

import com.clearspring.analytics.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.tencent.rss.client.util.ClientUtils;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.io.CompressionCodec;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.RssShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteBufferManager extends MemoryConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(WriteBufferManager.class);

  private int bufferSize;
  private long spillSize;
  // allocated bytes from executor memory
  private long allocatedBytes;
  private int executorId;
  private int shuffleId;
  private SerializerInstance instance;
  private ShuffleWriteMetrics shuffleWriteMetrics;
  // cache partition -> records
  private Map<Integer, WriterBuffer> buffers;
  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private int serializerBufferSize;
  private int serializerMaxBufferSize;
  private long copyTime;
  private CompressionCodec compressionCodec;

  public WriteBufferManager(
      int shuffleId,
      int executorId,
      BufferManagerOptions bufferManagerOptions,
      Serializer serializer,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      TaskMemoryManager taskMemoryManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      CompressionCodec compressionCodec) {
    super(taskMemoryManager);
    this.bufferSize = bufferManagerOptions.getBufferSize();
    this.spillSize = bufferManagerOptions.getBufferSpillThreshold();
    this.executorId = executorId;
    this.instance = serializer.newInstance();
    this.buffers = Maps.newHashMap();
    this.allocatedBytes = 0;
    this.copyTime = 0;
    this.shuffleId = shuffleId;
    this.partitionToServers = partitionToServers;
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.serializerBufferSize = bufferManagerOptions.getSerializerBufferSize();
    this.serializerMaxBufferSize = bufferManagerOptions.getSerializerBufferMax();
    this.compressionCodec = compressionCodec;
  }

  // add record to cache, return [partition, ShuffleBlock] if meet spill condition
  public List<ShuffleBlockInfo> addRecord(int partitionId, Object key, Object value) {
    List<ShuffleBlockInfo> result = Lists.newArrayList();
    if (buffers.containsKey(partitionId)) {
      WriterBuffer wb = buffers.get(partitionId);
      int[] addResult = wb.addRecord(key, value);
      requestMemory(addResult[WriterBuffer.INDEX_EXTRA_MEMORY]);
      int length = wb.getLength();
      if (length > bufferSize) {
        result.add(createShuffleBlock(partitionId, wb.getData(), wb.getMemorySize()));
        wb.clear();
        copyTime += wb.getCopyTime();
        buffers.remove(partitionId);
        LOG.info("Single buffer is full for shuffleId[" + shuffleId
            + "] partition[" + partitionId + "] with " + length + " bytes");
      }
    } else {
      requestMemory(serializerMaxBufferSize);
      WriterBuffer wb = new WriterBuffer(instance, serializerBufferSize, serializerMaxBufferSize);
      int[] addResult = wb.addRecord(key, value);
      int length = addResult[WriterBuffer.INDEX_RECORD_SIZE];
      if (length > bufferSize) {
        // request here just for calculate accuracy when release the memory
        requestMemory(length);
        result.add(createShuffleBlock(partitionId, wb.getData(), wb.getMemorySize()));
        wb.clear();
        copyTime += wb.getCopyTime();
        LOG.info("Single buffer is full for shuffleId[" + shuffleId
            + "] partition[" + partitionId + "] with " + length + " bytes");
      } else {
        buffers.put(partitionId, wb);
      }
    }
    shuffleWriteMetrics.incRecordsWritten(1L);

    // check buffer size > spill threshold
    if (allocatedBytes > spillSize) {
      result = clear();
    }

    return result;
  }

  // transform all [partition, records] to [partition, ShuffleBlockInfo] and clear cache
  public List<ShuffleBlockInfo> clear() {
    List<ShuffleBlockInfo> result = Lists.newArrayList();
    for (Entry<Integer, WriterBuffer> entry : buffers.entrySet()) {
      WriterBuffer wb = entry.getValue();
      if (wb.getLength() > 0) {
        result.add(createShuffleBlock(entry.getKey(), wb.getData(), wb.getMemorySize()));
      }
      wb.clear();
      copyTime += wb.getCopyTime();
    }
    LOG.info("Flush total buffer for shuffleId[" + shuffleId + "] with allocated[" + allocatedBytes + "]");
    buffers.clear();
    allocatedBytes = 0;
    return result;
  }

  // transform records to shuffleBlock
  private ShuffleBlockInfo createShuffleBlock(int partitionId, byte[] data, int freeMemory) {
    long uncompressLength = data.length;
    byte[] compressed = RssShuffleUtils.compressData(compressionCodec, data);
    long crc32 = ChecksumUtils.getCrc32(compressed);
    shuffleWriteMetrics.incBytesWritten(compressed.length);
    // just free memory from buffer manager, doesn't return to Executor now,
    // it will happen after send block to shuffle server
    allocatedBytes -= freeMemory;
    return new ShuffleBlockInfo(shuffleId, partitionId,
        ClientUtils.getBlockId(executorId, ClientUtils.getAtomicInteger()),
        compressed.length, crc32, compressed, partitionToServers.get(partitionId), uncompressLength);
  }

  private void requestMemory(long requiredMem) {
    if (requiredMem == 0) {
      return;
    }
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
            + " WriteBufferManager used[" + allocatedBytes + "] task used[" + used
            + "], consider to optimize 'spark.executor.memory',"
            + " 'spark.rss.writer.buffer.spill.size'.";
        LOG.error(message);
        throw new OutOfMemoryError(message);
      }
    }
    allocatedBytes += requiredMem;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) {
    // there is no spill for such situation
    return 0;
  }

  @VisibleForTesting
  public long getAllocatedBytes() {
    return allocatedBytes;
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

  public long getCopyTime() {
    return copyTime;
  }
}
