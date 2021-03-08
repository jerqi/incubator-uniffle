package org.apache.spark.shuffle.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.handler.impl.HdfsShuffleWriteHandler;
import com.tencent.rss.storage.util.StorageType;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.AbstractRssReaderTest;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class RssShuffleDataIteratorTest extends AbstractRssReaderTest {

  private static final Serializer KRYO_SERIALIZER = new KryoSerializer(new SparkConf(false));
  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";

  @Test
  public void readTest1() throws Exception {
    String basePath = HDFS_URI + "readTest1";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test1", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
        10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(KRYO_SERIALIZER, readClient,
        new ShuffleReadMetrics());

    validateResult(rssShuffleDataIterator, expectedData, 10);

    expectedBlockIds.add(-1L);
    try {
      // can't find all expected block id, data loss
      new ShuffleReadClientImpl(
          StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
          10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Can't find blockIds"));
    }
  }

  @Test
  public void readTest2() throws Exception {
    String basePath = HDFS_URI + "readTest2";
    HdfsShuffleWriteHandler writeHandler1 =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test2_1", conf);
    HdfsShuffleWriteHandler writeHandler2 =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test2_2", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler1, 2, 5, expectedData,
        expectedBlockIds, "key1", KRYO_SERIALIZER);
    writeTestData(writeHandler2, 2, 5, expectedData,
        expectedBlockIds, "key2", KRYO_SERIALIZER);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
        10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());

    validateResult(rssShuffleDataIterator, expectedData, 20);
    assertEquals(20, rssShuffleDataIterator.getShuffleReadMetrics().recordsRead());
    assertEquals(256, rssShuffleDataIterator.getShuffleReadMetrics().remoteBytesRead());
    assertTrue(rssShuffleDataIterator.getShuffleReadMetrics().fetchWaitTime() > 0);
  }

  @Test
  public void readTest3() throws Exception {
    String basePath = HDFS_URI + "readTest3";
    HdfsShuffleWriteHandler writeHandler1 =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test3_1", conf);
    HdfsShuffleWriteHandler writeHandler2 =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test3_2", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler1, 2, 5, expectedData,
        expectedBlockIds, "key1", KRYO_SERIALIZER);
    writeTestData(writeHandler2, 2, 5, expectedData,
        expectedBlockIds, "key2", KRYO_SERIALIZER);

    // duplicate file created, it should be used in product environment
    String shuffleFolder = basePath + "/appId/0/0-1";
    FileUtil.copy(fs, new Path(shuffleFolder + "/test3_1.data"), fs,
        new Path(shuffleFolder + "/test3_1.cp.data"), false, conf);
    FileUtil.copy(fs, new Path(shuffleFolder + "/test3_1.index"), fs,
        new Path(shuffleFolder + "/test3_1.cp.index"), false, conf);
    FileUtil.copy(fs, new Path(shuffleFolder + "/test3_2.data"), fs,
        new Path(shuffleFolder + "/test3_2.cp.data"), false, conf);
    FileUtil.copy(fs, new Path(shuffleFolder + "/test3_2.index"), fs,
        new Path(shuffleFolder + "/test3_2.cp.index"), false, conf);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
        10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());

    validateResult(rssShuffleDataIterator, expectedData, 20);
  }

  @Test
  public void readTest4() throws Exception {
    String basePath = HDFS_URI + "readTest4";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test1", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
        10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    // data file is deleted after iterator initialization
    Path dataFile = new Path(basePath + "/appId/0/0-1/test1.data");
    fs.delete(dataFile, true);
    // sleep to wait delete operation
    Thread.sleep(10000);
    try {
      fs.listStatus(dataFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
    }

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
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
        10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    // index file is deleted after iterator initialization, it should be ok, all index infos are read already
    Path indexFile = new Path(basePath + "/appId/0/0-1/test.index");
    fs.delete(indexFile, true);
    // sleep to wait delete operation
    Thread.sleep(10000);
    try {
      fs.listStatus(indexFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
    }

    validateResult(rssShuffleDataIterator, expectedData, 10);
  }

  @Test
  public void readTest6() throws Exception {
    String basePath = HDFS_URI + "readTest6";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);
    // index file is deleted before iterator initialization
    Path indexFile = new Path(basePath + "/appId/0/0-1/test.index");
    fs.delete(indexFile, true);
    // sleep to wait delete operation
    Thread.sleep(10000);
    try {
      fs.listStatus(indexFile);
      fail("Index file should be deleted");
    } catch (Exception e) {
    }

    try {
      new ShuffleReadClientImpl(
          StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
          10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
      fail(EXPECTED_EXCEPTION_MESSAGE);
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("No index file found"));
    }
  }

  @Test
  public void readTest7() throws Exception {
    String basePath = HDFS_URI + "readTest7";
    HdfsShuffleWriteHandler writeHandler =
        new HdfsShuffleWriteHandler("appId", 0, 0, 1, basePath, "test", conf);

    Map<String, String> expectedData = Maps.newHashMap();
    Set<Long> expectedBlockIds = Sets.newHashSet();
    writeTestData(writeHandler, 2, 5, expectedData,
        expectedBlockIds, "key", KRYO_SERIALIZER);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
        10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());

    // crc32 is incorrect
    try (MockedStatic<ChecksumUtils> checksumUtilsMock = Mockito.mockStatic(ChecksumUtils.class)) {
      checksumUtilsMock.when(() -> ChecksumUtils.getCrc32((ByteBuffer) any())).thenReturn(-1L);
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
  public void readTest8() {
    String basePath = HDFS_URI + "readTest8";
    Set<Long> expectedBlockIds = Sets.newHashSet();
    // there is no data for basePath
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(
        StorageType.HDFS.name(), "appId", 0, 1, 100, 2,
        10, 10000, basePath, expectedBlockIds, Lists.newArrayList());
    RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
        KRYO_SERIALIZER, readClient, new ShuffleReadMetrics());
    assertFalse(rssShuffleDataIterator.hasNext());
  }
}
