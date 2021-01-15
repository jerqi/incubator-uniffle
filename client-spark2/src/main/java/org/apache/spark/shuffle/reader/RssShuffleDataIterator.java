package org.apache.spark.shuffle.reader;

import com.esotericsoftware.kryo.io.Input;
import com.tecent.rss.client.ShuffleReadClient;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
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

  public RssShuffleDataIterator(Serializer serializer, ShuffleReadClient shuffleReadClient) {
    this.serializerInstance = serializer.newInstance();
    this.shuffleReadClient = shuffleReadClient;
  }

  public void checkExpectedBlockIds() {
    shuffleReadClient.checkExpectedBlockIds();
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
      byte[] data = shuffleReadClient.readShuffleData();
      if (data != null) {
        // create new iterator for shuffle data
        recordsIterator = createKVIterator(data);
      } else {
        // finish reading records, close related reader and check data consistent
        shuffleReadClient.close();
        shuffleReadClient.checkProcessedBlockIds();
        return false;
      }
    }
    return recordsIterator.hasNext();
  }

  @Override
  public Product2<K, C> next() {
    return (Product2<K, C>) recordsIterator.next();
  }

}

