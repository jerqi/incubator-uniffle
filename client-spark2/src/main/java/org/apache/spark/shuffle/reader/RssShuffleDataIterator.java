package org.apache.spark.shuffle.reader;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.RssShuffleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Product2;
import scala.Tuple2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;

public class RssShuffleDataIterator<K, C> extends AbstractIterator<Product2<K, C>> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleDataIterator.class);

  private Iterator<Tuple2<Object, Object>> recordsIterator = null;
  private SerializerInstance serializerInstance;
  private ShuffleReadClient shuffleReadClient;
  private ShuffleReadMetrics shuffleReadMetrics;
  private long readTime = 0;
  private long serializeTime = 0;
  private long decompressTime = 0;
  private Input deserializationInput = null;
  private DeserializationStream deserializationStream = null;


  public RssShuffleDataIterator(
      Serializer serializer,
      ShuffleReadClient shuffleReadClient,
      ShuffleReadMetrics shuffleReadMetrics) {
    this.serializerInstance = serializer.newInstance();
    this.shuffleReadClient = shuffleReadClient;
    this.shuffleReadMetrics = shuffleReadMetrics;
  }

  public Iterator<Tuple2<Object, Object>> createKVIterator(byte[] data) {
    clearDeserializationStream();
    deserializationInput = new Input(data, 0, data.length);
    deserializationStream = serializerInstance.deserializeStream(deserializationInput);
    return deserializationStream.asKeyValueIterator();
  }

  private void clearDeserializationStream() {
    if (deserializationInput != null) {
      deserializationInput.close();
    }
    if (deserializationStream != null) {
      deserializationStream.close();
    }
    deserializationInput = null;
    deserializationStream = null;
  }

  @Override
  public boolean hasNext() {
    if (recordsIterator == null || !recordsIterator.hasNext()) {
      // read next segment
      long startFetch = System.currentTimeMillis();
      CompressedShuffleBlock compressedBlock = shuffleReadClient.readShuffleBlockData();
      byte[] compressedData = null;
      if (compressedBlock != null) {
        compressedData = compressedBlock.getCompressData();
      }
      long fetchDuration = System.currentTimeMillis() - startFetch;
      shuffleReadMetrics.incFetchWaitTime(fetchDuration);
      if (compressedData != null) {
        int compressedLength = compressedData.length;
        long startDecompress = System.currentTimeMillis();
        byte[] uncompressedData = RssShuffleUtils.decompressData(
            compressedData, compressedBlock.getUncompressLength());
        long decompressDuration = System.currentTimeMillis() - startDecompress;
        decompressTime += decompressDuration;
        // create new iterator for shuffle data
        long startSerialization = System.currentTimeMillis();
        recordsIterator = createKVIterator(uncompressedData);
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        shuffleReadMetrics.incRemoteBytesRead(compressedLength);
        readTime += fetchDuration;
        serializeTime += serializationDuration;
      } else {
        // finish reading records, close related reader and check data consistent
        shuffleReadClient.close();
        shuffleReadClient.checkProcessedBlockIds();
        shuffleReadClient.logStatics();
        LOG.info("Fetch " + shuffleReadMetrics.remoteBytesRead() + " bytes cost " + readTime + " ms and "
            + serializeTime + " ms to serialize, " + decompressTime + " ms to decompress.");
        return false;
      }
    }
    return recordsIterator.hasNext();
  }

  @Override
  public Product2<K, C> next() {
    shuffleReadMetrics.incRecordsRead(1L);
    return (Product2<K, C>) recordsIterator.next();
  }

  @VisibleForTesting
  protected ShuffleReadMetrics getShuffleReadMetrics() {
    return shuffleReadMetrics;
  }
}

