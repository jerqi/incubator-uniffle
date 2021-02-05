package org.apache.spark.shuffle.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.impl.FileBasedShuffleReadClient;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssReaderTestBase;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class RssShuffleDataIteratorTest extends RssReaderTestBase {

  private static final Serializer KRYO_SERIALIZER = new KryoSerializer(new SparkConf(false));
  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";

  @Test
  public void readTest1() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "readTest1";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test1", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(KRYO_SERIALIZER, readClient,
        new ShuffleReadMetrics());
    rssShuffleDataIterator.checkExpectedBlockIds();

    validateResult(rssShuffleDataIterator, expectedData, 10);

    expectedBlockIds.add(-1L);
    // can't find all expected block id, data loss
    readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    rssShuffleDataIterator = new RssShuffleDataIterator(KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    try {
      rssShuffleDataIterator.checkExpectedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Can't find blockIds"));
    }
  }

  @Test
  public void readTest2() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "readTest2";
    FileBasedShuffleWriteHandler writeHandler1 =
        new FileBasedShuffleWriteHandler(basePath, "test2_1", conf);
    FileBasedShuffleWriteHandler writeHandler2 =
        new FileBasedShuffleWriteHandler(basePath, "test2_2", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler1, 2, 5, expectedData,
        expectedBlockIds, "key1", KRYO_SERIALIZER);
    writeTestData(writeHandler2, 2, 5, expectedData,
        expectedBlockIds, "key2", KRYO_SERIALIZER);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    rssShuffleDataIterator.checkExpectedBlockIds();

    validateResult(rssShuffleDataIterator, expectedData, 20);
    assertEquals(20, rssShuffleDataIterator.getShuffleReadMetrics().recordsRead());
    assertEquals(540, rssShuffleDataIterator.getShuffleReadMetrics().remoteBytesRead());
    assertTrue(rssShuffleDataIterator.getShuffleReadMetrics().fetchWaitTime() > 0);
  }

  @Test
  public void readTest3() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "readTest3";
    FileBasedShuffleWriteHandler writeHandler1 =
        new FileBasedShuffleWriteHandler(basePath, "test3_1", conf);
    FileBasedShuffleWriteHandler writeHandler2 =
        new FileBasedShuffleWriteHandler(basePath, "test3_2", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler1, 2, 5, expectedData,
        expectedBlockIds, "key1", KRYO_SERIALIZER);
    writeTestData(writeHandler2, 2, 5, expectedData,
        expectedBlockIds, "key2", KRYO_SERIALIZER);

    // duplicate file created, it should be used in product environment
    FileUtil.copy(fs, new Path(basePath + "/test3_1.data"), fs,
        new Path(basePath + "/test3_1.cp.data"), false, conf);
    FileUtil.copy(fs, new Path(basePath + "/test3_1.index"), fs,
        new Path(basePath + "/test3_1.cp.index"), false, conf);
    FileUtil.copy(fs, new Path(basePath + "/test3_2.data"), fs,
        new Path(basePath + "/test3_2.cp.data"), false, conf);
    FileUtil.copy(fs, new Path(basePath + "/test3_2.index"), fs,
        new Path(basePath + "/test3_2.cp.index"), false, conf);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    rssShuffleDataIterator.checkExpectedBlockIds();

    validateResult(rssShuffleDataIterator, expectedData, 20);
  }

  @Test
  public void readTest4() throws Exception {
    String basePath = HDFS_URI + "readTest4";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test1", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    rssShuffleDataIterator.checkExpectedBlockIds();
    // data file is deleted after iterator initialization
    fs.delete(new Path(basePath + "/test1.data"), true);
    // sleep to wait delete operation
    Thread.sleep(10000);

    try {
      while (rssShuffleDataIterator.hasNext()) {
        rssShuffleDataIterator.next();
      }
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Blocks read inconsistent: expected"));
    }
  }

  @Test
  public void readTest5() throws Exception {
    String basePath = HDFS_URI + "readTest5";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    rssShuffleDataIterator.checkExpectedBlockIds();
    // index file is deleted after iterator initialization, it should be ok, all index infos are read already
    fs.delete(new Path(basePath + "/test.index"), true);
    // sleep to wait delete operation
    Thread.sleep(10000);

    validateResult(rssShuffleDataIterator, expectedData, 10);
  }

  @Test
  public void readTest6() throws Exception {
    String basePath = HDFS_URI + "readTest6";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);
    // index file is deleted before iterator initialization
    fs.delete(new Path(basePath + "/test.index"), true);
    // sleep to wait delete operation
    Thread.sleep(10000);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    try {
      rssShuffleDataIterator.checkExpectedBlockIds();
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("No index file found"));
    }
  }

  @Test
  public void readTest7() throws IOException, IllegalStateException {
    String basePath = HDFS_URI + "readTest7";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);

    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    rssShuffleDataIterator.checkExpectedBlockIds();

    // crc32 is incorrect
    try (MockedStatic<ChecksumUtils> checksumUtilsMock = Mockito.mockStatic(ChecksumUtils.class)) {
      checksumUtilsMock.when(() -> ChecksumUtils.getCrc32(any())).thenReturn(-1L);
      try {
        while (rssShuffleDataIterator.hasNext()) {
          rssShuffleDataIterator.next();
        }
        fail(EXPECTED_EXCEPTION_MESSAGE);
      } catch (Exception e) {
        assertTrue(e.getMessage().startsWith("Unexpected crc value"));
      }
    }
  }

  @Test
  public void readTest8() throws Exception {
    String basePath = HDFS_URI + "readTest8";
    FileBasedShuffleWriteHandler writeHandler =
        new FileBasedShuffleWriteHandler(basePath, "test", conf);
    Set<Long> expectedBlockIds = Sets.newHashSet();
    // there is no data for basePath
    FileBasedShuffleReadClient readClient = new FileBasedShuffleReadClient(
        basePath, conf, 100, 10000, expectedBlockIds);
    readClient.checkExpectedBlockIds();
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    assertFalse(rssShuffleDataIterator.hasNext());
  }
}
