package org.apache.spark.shuffle;

import static org.junit.Assert.assertEquals;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tecent.rss.client.ClientUtils;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import com.tencent.rss.storage.HdfsTestBase;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import scala.Product2;
import scala.collection.Iterator;
import scala.reflect.ClassTag$;

public class RssReaderTestBase extends HdfsTestBase {

    protected void validateResult(Iterator iterator,
            Map<String, String> expectedData, int recordNum) {
        Set<String> actualKeys = Sets.newHashSet();
        while (iterator.hasNext()) {
            Product2 product2 = (Product2) iterator.next();
            String key = (String) product2._1();
            String value = (String) product2._2();
            actualKeys.add(key);
            assertEquals(expectedData.get(key), value);
        }
        assertEquals(recordNum, actualKeys.size());
        assertEquals(expectedData.keySet(), actualKeys);
    }

    protected void writeTestData(FileBasedShuffleWriteHandler writeHandler,
            int blockNum, int recordNum, Map<String, String> expectedData,
            Set<Long> expectedBlockIds, String keyPrefix, Serializer serializer) throws IOException {
        List<ShufflePartitionedBlock> blocks = Lists.newArrayList();
        SerializerInstance serializerInstance = serializer.newInstance();
        for (int i = 0; i < blockNum; i++) {
            Output output = new Output(1024, 2048);
            SerializationStream serializeStream = serializerInstance.serializeStream(output);
            for (int j = 0; j < recordNum; j++) {
                String key = keyPrefix + "_" + i + "_" + j;
                String value = "valuePrefix_" + i + "_" + j;
                expectedData.put(key, value);
                writeData(serializeStream, key, value);
            }
            long blockId = ClientUtils.getBlockId(1, ClientUtils.getAtomicInteger());
            expectedBlockIds.add(blockId);
            blocks.add(createShuffleBlock(output.toBytes(), blockId));
            serializeStream.close();
        }
        writeHandler.write(blocks);
    }

    protected ShufflePartitionedBlock createShuffleBlock(byte[] data, long blockId) {
        long crc = ChecksumUtils.getCrc32(data);
        return new ShufflePartitionedBlock(data.length, crc, blockId, data);
    }

    protected void writeData(SerializationStream serializeStream, String key, String value) {
        serializeStream.writeKey(key, ClassTag$.MODULE$.apply(key.getClass()));
        serializeStream.writeValue(value, ClassTag$.MODULE$.apply(value.getClass()));
        serializeStream.flush();
    }

}