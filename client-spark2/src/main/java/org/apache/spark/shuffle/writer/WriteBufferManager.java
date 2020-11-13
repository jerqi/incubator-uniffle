package org.apache.spark.shuffle.writer;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.tecent.rss.client.ClientUtils;
import com.tencent.rss.proto.RssProtos;
import java.util.HashMap;
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
    private SerializerInstance instance;
    // cache partition -> records
    private Map<Integer, WriterBuffer> buffers;

    public WriteBufferManager(int bufferSize, int maxBufferSize, int spillSize,
            int executorId, Serializer serializer) {
        this.bufferSize = bufferSize;
        this.maxBufferSize = maxBufferSize;
        this.spillSize = spillSize;
        this.executorId = executorId;
        this.instance = serializer.newInstance();
        this.buffers = new HashMap<>();
        this.totalBytes = 0L;
    }

    // add record to cache, return [partition, ShuffleBlock] if meet spill condition
    public Map<Integer, RssProtos.ShuffleBlock> addRecord(int partitionId, Object key, Object value) {
        Map<Integer, RssProtos.ShuffleBlock> result = new HashMap<>();
        if (buffers.containsKey(partitionId)) {
            SerializationStream serializeStream = buffers.get(partitionId).getSerializeStream();
            Output output = buffers.get(partitionId).getOutput();
            int oldSize = output.position();
            serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
            serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
            serializeStream.flush();
            int newSize = output.position();
            if (newSize >= bufferSize) {
                result.put(partitionId, createShuffleBlock(output.toBytes()));
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
                result.put(partitionId, createShuffleBlock(output.toBytes()));
                serializeStream.close();
            } else {
                buffers.put(partitionId, new WriterBuffer(serializeStream, output));
                totalBytes = totalBytes + newSize;
            }
        }

        // check buffer size > spill threshold
        if (totalBytes >= spillSize) {
            for (Entry<Integer, WriterBuffer> entry : buffers.entrySet()) {
                result.put(entry.getKey(), createShuffleBlock(entry.getValue().getOutput().toBytes()));
                entry.getValue().getSerializeStream().close();
            }
            buffers.clear();
            totalBytes = 0;
        }

        return result;
    }

    // transform all [partition, records] to [partition, ShuffleBlock] and clear cache
    public Map<Integer, RssProtos.ShuffleBlock> clear() {
        Map<Integer, RssProtos.ShuffleBlock> result = new HashMap<>();
        for (Entry<Integer, WriterBuffer> entry : buffers.entrySet()) {
            result.put(entry.getKey(), createShuffleBlock(entry.getValue().getOutput().toBytes()));
            entry.getValue().getSerializeStream().close();
        }
        buffers.clear();
        totalBytes = 0;
        return result;
    }

    // transform records to shuffleBlock
    private RssProtos.ShuffleBlock createShuffleBlock(byte[] data) {
        return RssProtos.ShuffleBlock.newBuilder().setData(ByteString.copyFrom(data))
                .setBlockId(ClientUtils.getBlockId(executorId, ClientUtils.getAtomicInteger()))
                .setCrc(0)
                .setLength(data.length).build();
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
