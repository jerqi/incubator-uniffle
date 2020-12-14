package org.apache.spark.shuffle.writer;

import com.clearspring.analytics.util.Lists;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.tecent.rss.client.ClientUtils;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.reflect.ClassTag$;

public class WriteBufferManager {

    private int bufferSize;
    private int maxBufferSize;
    private int spillSize;
    private long totalBytes;
    private int executorId;
    private int shuffleId;
    private SerializerInstance instance;
    // cache partition -> records
    private Map<Integer, WriterBuffer> buffers;
    private Map<Integer, List<ShuffleServerInfo>> partitionToServers;

    public WriteBufferManager(int shuffleId, int bufferSize, int maxBufferSize, int spillSize,
            int executorId, Serializer serializer, Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
        this.bufferSize = bufferSize;
        this.maxBufferSize = maxBufferSize;
        this.spillSize = spillSize;
        this.executorId = executorId;
        this.instance = serializer.newInstance();
        this.buffers = Maps.newHashMap();
        this.totalBytes = 0L;
        this.shuffleId = shuffleId;
        this.partitionToServers = partitionToServers;
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
            if (newSize >= bufferSize) {
                result.add(createShuffleBlock(partitionId, output.toBytes()));
                serializeStream.close();
                buffers.remove(partitionId);
                totalBytes -= oldSize;
            } else {
                totalBytes += (newSize - oldSize);
            }
        } else {
            Output output = new Output(bufferSize, maxBufferSize);
            SerializationStream serializeStream = instance.serializeStream(output);
            serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
            serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
            serializeStream.flush();
            int newSize = output.position();
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

    @VisibleForTesting
    protected long getTotalBytes() {
        return totalBytes;
    }

    @VisibleForTesting
    protected Map<Integer, WriterBuffer> getBuffers() {
        return buffers;
    }
}
