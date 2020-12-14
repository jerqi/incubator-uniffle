package org.apache.spark.shuffle;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.storage.FileBasedShuffleWriteHandler;
import java.util.Map;
import java.util.Set;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.junit.Test;
import scala.Option;

public class RssShuffleReaderTest extends RssReaderTestBase {

    private static final Serializer KRYO_SERIALIZER = new KryoSerializer(new SparkConf(false));

    @Test
    public void readTest() throws Exception {

        String basePath = HDFS_URI + "readTest1";
        FileBasedShuffleWriteHandler writeHandler =
                new FileBasedShuffleWriteHandler(basePath, "test", conf);

        Map<String, String> expectedData = Maps.newHashMap();
        Set<Long> expectedBlockIds = Sets.newHashSet();
        writeTestData(writeHandler, 2, 5, expectedData,
                expectedBlockIds, "key", KRYO_SERIALIZER);

        TaskContext contextMock = mock(TaskContext.class);
        RssShuffleHandle handleMock = mock(RssShuffleHandle.class);
        ShuffleDependency dependencyMock = mock(ShuffleDependency.class);
        when(handleMock.getAppId()).thenReturn("testAppId");
        when(handleMock.getShuffleId()).thenReturn(1);
        when(handleMock.getDependency()).thenReturn(dependencyMock);
        when(dependencyMock.serializer()).thenReturn(KRYO_SERIALIZER);
        when(contextMock.taskAttemptId()).thenReturn(1L);
        when(contextMock.attemptNumber()).thenReturn(1);
        doNothing().when(contextMock).killTaskIfInterrupted();
        when(dependencyMock.mapSideCombine()).thenReturn(false);
        when(dependencyMock.aggregator()).thenReturn(Option.empty());
        when(dependencyMock.keyOrdering()).thenReturn(Option.empty());

        RssShuffleReader rssShuffleReaderSpy = spy(new RssShuffleReader<String, String>(0, 1, contextMock,
                handleMock, 0, basePath, 1000, conf));
        doReturn(expectedBlockIds).when(rssShuffleReaderSpy).getExpectedBlockIds();

        validateResult(rssShuffleReaderSpy.read(), expectedData, 10);
    }
}
