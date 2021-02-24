package org.apache.spark.shuffle.reader;

import com.esotericsoftware.kryo.io.Input;
import com.google.common.annotations.VisibleForTesting;
import com.tencent.rss.client.api.ShuffleReadClient;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.io.CompressionCodec;
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
  private CompressionCodec compressionCodec;
  private int compressionBlockSize;

  public RssShuffleDataIterator(
      Serializer serializer,
      ShuffleReadClient shuffleReadClient,
      ShuffleReadMetrics shuffleReadMetrics,
      CompressionCodec compressionCodec,
      int compressionBlockSize) {
    this.serializerInstance = serializer.newInstance();
    this.shuffleReadClient = shuffleReadClient;
    this.shuffleReadMetrics = shuffleReadMetrics;
    this.compressionCodec = compressionCodec;
    this.compressionBlockSize = compressionBlockSize;
  }

  public Iterator<Tuple2<Object, Object>> createKVIterator(byte[] data) {
    Input deserializationInput = new Input(data, 0, data.length);
    DeserializationStream ds = serializerInstance.deserializeStream(deserializationInput);
    return ds.asKeyValueIterator();
  }

  @Override
  public boolean hasNext() {
    if (recordsIterator == null || !recordsIterator.hasNext()) {
      // read next segment
      long startFetch = System.currentTimeMillis();
      byte[] compressedData = shuffleReadClient.readShuffleBlockData();
      long fetchDuration = System.currentTimeMillis() - startFetch;
      shuffleReadMetrics.incFetchWaitTime(fetchDuration);
      if (compressedData != null) {
        int compressedLength = compressedData.length;
        long startDecompress = System.currentTimeMillis();
        byte[] uncompressedData = RssShuffleUtils.decompressData(
            compressionCodec, compressedData, compressionBlockSize);
        long decompressDuration = System.currentTimeMillis() - startDecompress;
        // create new iterator for shuffle data
        long startSerialization = System.currentTimeMillis();
        recordsIterator = createKVIterator(uncompressedData);
        long serializationDuration = System.currentTimeMillis() - startSerialization;
        shuffleReadMetrics.incRemoteBytesRead(compressedLength);
        LOG.info("Fetch " + compressedLength + " bytes cost " + fetchDuration + "ms to fetch and "
            + serializationDuration + " ms to serialize, " + decompressDuration + " ms to decompress");
      } else {
        // finish reading records, close related reader and check data consistent
        shuffleReadClient.close();
        shuffleReadClient.checkProcessedBlockIds();
        shuffleReadClient.logStatics();
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

