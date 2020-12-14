package org.apache.spark.shuffle.reader;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
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

        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
                100, KRYO_SERIALIZER, expectedBlockIds);
        rssShuffleDataIterator.init(basePath, conf);

        validateResult(rssShuffleDataIterator, expectedData, 10);

        expectedBlockIds.add(-1L);
        // can't find all expected block id, data loss
        rssShuffleDataIterator = new RssShuffleDataIterator(100, KRYO_SERIALIZER, expectedBlockIds);
        try {
            rssShuffleDataIterator.init(basePath, conf);
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

        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
                100, KRYO_SERIALIZER, expectedBlockIds);
        rssShuffleDataIterator.init(basePath, conf);

        validateResult(rssShuffleDataIterator, expectedData, 20);
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

        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
                100, KRYO_SERIALIZER, expectedBlockIds);
        rssShuffleDataIterator.init(basePath, conf);

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

        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
                100, KRYO_SERIALIZER, expectedBlockIds);
        rssShuffleDataIterator.init(basePath, conf);
        // data file is deleted after iterator initialization
        fs.delete(new Path(basePath + "/test1.data"), true);
        // sleep to wait delete operation
        Thread.sleep(10000);

        try {
            rssShuffleDataIterator.hasNext();
            fail(EXPECTED_EXCEPTION_MESSAGE);
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Can't read data with blockId"));
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

        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
                100, KRYO_SERIALIZER, expectedBlockIds);
        rssShuffleDataIterator.init(basePath, conf);
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

        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
                100, KRYO_SERIALIZER, expectedBlockIds);
        try {
            rssShuffleDataIterator.init(basePath, conf);
            fail(EXPECTED_EXCEPTION_MESSAGE);
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("No index file found"));
        }
    }

    @Test
    public void readTest7() throws Exception {
        String basePath = HDFS_URI + "readTest7";
        FileBasedShuffleWriteHandler writeHandler =
                new FileBasedShuffleWriteHandler(basePath, "test", conf);

        Map<String, String> expectedData = Maps.newHashMap();
        Set<Long> expectedBlockIds = Sets.newHashSet();
        writeTestData(writeHandler, 3, 5, expectedData,
                expectedBlockIds, "key", KRYO_SERIALIZER);

        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
                100, KRYO_SERIALIZER, expectedBlockIds);
        rssShuffleDataIterator.init(basePath, conf);

        // discard one block
        rssShuffleDataIterator.getBlockIdQueue().poll();

        try {
            while (rssShuffleDataIterator.hasNext()) {
                rssShuffleDataIterator.next();
            }
            fail(EXPECTED_EXCEPTION_MESSAGE);
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Blocks read inconsistent:"));
        }
    }

    @Test
    public void readTest8() throws IOException, IllegalStateException {
        String basePath = HDFS_URI + "readTest8";
        FileBasedShuffleWriteHandler writeHandler =
                new FileBasedShuffleWriteHandler(basePath, "test", conf);

        Map<String, String> expectedData = Maps.newHashMap();
        Set<Long> expectedBlockIds = Sets.newHashSet();
        writeTestData(writeHandler, 2, 5, expectedData,
                expectedBlockIds, "key", KRYO_SERIALIZER);

        RssShuffleDataIterator rssShuffleDataIterator = new RssShuffleDataIterator(
                100, KRYO_SERIALIZER, expectedBlockIds);
        rssShuffleDataIterator.init(basePath, conf);

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
}
