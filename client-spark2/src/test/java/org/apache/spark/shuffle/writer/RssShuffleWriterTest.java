package org.apache.spark.shuffle.writer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tecent.rss.client.ShuffleWriteClient;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.util.EventLoop;
import org.hamcrest.core.StringStartsWith;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import scala.Product2;
import scala.Tuple2;
import scala.collection.mutable.MutableList;

public class RssShuffleWriterTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void checkBlockSendResultTest() {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp")
        .setMaster("local[2]")
        .set("spark.rss.test", "true")
        .set("spark.rss.writer.send.check.timeout", "10000")
        .set("spark.rss.writer.send.check.interval", "1000")
        .set("spark.rss.coordinator.ip", "127.0.0.1");
    // init SparkContext
    SparkContext sc = SparkContext.getOrCreate(conf);
    RssShuffleManager manager = new RssShuffleManager(conf, false);

    Serializer mockSerializer = mock(Serializer.class);
    ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    Partitioner mockPartitioner = mock(Partitioner.class);
    ShuffleDependency mockDependency = mock(ShuffleDependency.class);
    RssShuffleHandle mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    when(mockDependency.serializer()).thenReturn(mockSerializer);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(2);
    when(mockHandle.getPartitionToServers()).thenReturn(Maps.newHashMap());
    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager bufferManager = new WriteBufferManager(
        0, 0, bufferOptions, mockSerializer,
        Maps.newHashMap(), mockTaskMemoryManager);
    WriteBufferManager bufferManagerSpy = spy(bufferManager);
    doReturn(1000000L).when(bufferManagerSpy).acquireMemory(anyLong());

    RssShuffleWriter rssShuffleWriter = new RssShuffleWriter("appId", 0, "taskId",
        bufferManagerSpy, (new TaskMetrics()).shuffleWriteMetrics(),
        mockDependency, manager, conf, mockShuffleWriteClient);

    // case 1: all blocks are sent successfully
    manager.addSuccessBlockIds("taskId", Sets.newHashSet(1L, 2L, 3L));
    rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L));
    manager.clearCachedBlockIds();

    // case 2: partial blocks aren't sent before spark.rss.writer.send.check.timeout,
    // Runtime exception will be thrown
    manager.addSuccessBlockIds("taskId", Sets.newHashSet(1L, 2L));
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(StringStartsWith.startsWith("Timeout:"));
    rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L));

    manager.clearCachedBlockIds();

    // case 3: partial blocks are sent failed, Runtime exception will be thrown
    manager.addSuccessBlockIds("taskId", Sets.newHashSet(1L, 2L));
    manager.addFailedBlockIds("taskId", Sets.newHashSet(3L));
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(StringStartsWith.startsWith("Send failed:"));
    rssShuffleWriter.checkBlockSendResult(Sets.newHashSet(1L, 2L, 3L));
    manager.clearCachedBlockIds();

    sc.stop();
  }

  @Test
  public void writeTest() throws Exception {
    SparkConf conf = new SparkConf();
    conf.setAppName("testApp").setMaster("local[2]")
        .set("spark.rss.test", "true")
        .set("spark.rss.writer.buffer.size", "32")
        .set("spark.rss.writer.buffer.max.size", "64")
        .set("spark.rss.writer.buffer.spill.size", "64")
        .set("spark.rss.writer.send.check.timeout", "10000")
        .set("spark.rss.writer.send.check.interval", "1000")
        .set("spark.rss.coordinator.ip", "127.0.0.1");
    // init SparkContext
    SparkContext sc = SparkContext.getOrCreate(conf);
    RssShuffleManager manager = new RssShuffleManager(conf, false);
    List<ShuffleBlockInfo> shuffleBlockInfos = Lists.newArrayList();

    manager.setEventLoop(new EventLoop<AddBlockEvent>("test") {
      @Override
      public void onReceive(AddBlockEvent event) {
        assertEquals("taskId", event.getTaskId());
        shuffleBlockInfos.addAll(event.getShuffleDataInfoList());
        Set<Long> blockIds = event.getShuffleDataInfoList().parallelStream()
            .map(sdi -> sdi.getBlockId()).collect(Collectors.toSet());
        manager.addSuccessBlockIds(event.getTaskId(), blockIds);
      }

      @Override
      public void onError(Throwable e) {
      }
    });
    manager.getEventLoop().start();

    Partitioner mockPartitioner = mock(Partitioner.class);
    ShuffleDependency mockDependency = mock(ShuffleDependency.class);
    ShuffleWriteClient mockShuffleWriteClient = mock(ShuffleWriteClient.class);
    RssShuffleHandle mockHandle = mock(RssShuffleHandle.class);
    when(mockHandle.getDependency()).thenReturn(mockDependency);
    Serializer kryoSerializer = new KryoSerializer(conf);
    when(mockDependency.serializer()).thenReturn(kryoSerializer);
    when(mockDependency.partitioner()).thenReturn(mockPartitioner);
    when(mockPartitioner.numPartitions()).thenReturn(2);

    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    List<ShuffleServerInfo> ssi12 = Arrays.asList(new ShuffleServerInfo("id1", "0.0.0.1", 100),
        new ShuffleServerInfo("id2", "0.0.0.2", 100));
    partitionToServers.put(0, ssi12);
    List<ShuffleServerInfo> ssi34 = Arrays.asList(new ShuffleServerInfo("id3", "0.0.0.3", 100),
        new ShuffleServerInfo("id4", "0.0.0.4", 100));
    partitionToServers.put(1, ssi34);
    List<ShuffleServerInfo> ssi56 = Arrays.asList(new ShuffleServerInfo("id5", "0.0.0.5", 100),
        new ShuffleServerInfo("id6", "0.0.0.6", 100));
    partitionToServers.put(2, ssi56);
    when(mockPartitioner.getPartition("testKey1")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey2")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey3")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey4")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey5")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey6")).thenReturn(2);
    when(mockPartitioner.getPartition("testKey7")).thenReturn(0);
    when(mockPartitioner.getPartition("testKey8")).thenReturn(1);
    when(mockPartitioner.getPartition("testKey9")).thenReturn(2);

    TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

    BufferManagerOptions bufferOptions = new BufferManagerOptions(conf);
    WriteBufferManager bufferManager = new WriteBufferManager(
        0, 0, bufferOptions, kryoSerializer,
        partitionToServers, mockTaskMemoryManager);
    WriteBufferManager bufferManagerSpy = spy(bufferManager);
    doReturn(1000000L).when(bufferManagerSpy).acquireMemory(anyLong());

    RssShuffleWriter rssShuffleWriter = new RssShuffleWriter("appId", 0, "taskId",
        bufferManagerSpy, (new TaskMetrics()).shuffleWriteMetrics(),
        mockDependency, manager, conf, mockShuffleWriteClient);

    RssShuffleWriter<String, String, String> rssShuffleWriterSpy = spy(rssShuffleWriter);
    doNothing().when(rssShuffleWriterSpy).sendCommit();

    // case 1
    MutableList<Product2<String, String>> data = new MutableList();
    data.appendElem(new Tuple2("testKey1", "testValue1"));
    data.appendElem(new Tuple2("testKey2", "testValue2"));
    data.appendElem(new Tuple2("testKey3", "testValue3"));
    data.appendElem(new Tuple2("testKey4", "testValue4"));
    data.appendElem(new Tuple2("testKey5", "testValue5"));
    data.appendElem(new Tuple2("testKey6", "testValue6"));
    rssShuffleWriterSpy.write(data.iterator());

    assertEquals(6, shuffleBlockInfos.size());
    for (ShuffleBlockInfo shuffleBlockInfo : shuffleBlockInfos) {
      assertEquals(0, shuffleBlockInfo.getShuffleId());
      assertEquals(22, shuffleBlockInfo.getLength());
      if (shuffleBlockInfo.getPartitionId() == 0) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi12);
      } else if (shuffleBlockInfo.getPartitionId() == 1) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi34);
      } else if (shuffleBlockInfo.getPartitionId() == 2) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi56);
      } else {
        throw new Exception("Shouldn't be here");
      }
    }
    Map<Integer, Set<Long>> partitionToBlockIds = rssShuffleWriterSpy.getPartitionToBlockIds();
    assertEquals(2, partitionToBlockIds.get(0).size());
    assertEquals(2, partitionToBlockIds.get(1).size());
    assertEquals(2, partitionToBlockIds.get(2).size());
    partitionToBlockIds.clear();

    // case2
    shuffleBlockInfos.clear();
    data = new MutableList();
    data.appendElem(new Tuple2("testKey1", "testValue1"));
    data.appendElem(new Tuple2("testKey4", "testValue4"));
    data.appendElem(new Tuple2("testKey2", "testValue2"));
    data.appendElem(new Tuple2("testKey3", "testValue3"));
    data.appendElem(new Tuple2("testKey5", "testValue6"));
    data.appendElem(new Tuple2("testKey6", "testValue5"));
    rssShuffleWriterSpy.write(data.iterator());

    assertEquals(3, shuffleBlockInfos.size());
    for (ShuffleBlockInfo shuffleBlockInfo : shuffleBlockInfos) {
      assertEquals(0, shuffleBlockInfo.getShuffleId());
      assertEquals(44, shuffleBlockInfo.getLength());
      if (shuffleBlockInfo.getPartitionId() == 0) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi12);
      } else if (shuffleBlockInfo.getPartitionId() == 1) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi34);
      } else if (shuffleBlockInfo.getPartitionId() == 2) {
        assertEquals(shuffleBlockInfo.getShuffleServerInfos(), ssi56);
      } else {
        throw new Exception("Shouldn't be here");
      }
    }
    partitionToBlockIds = rssShuffleWriterSpy.getPartitionToBlockIds();
    assertEquals(1, partitionToBlockIds.get(0).size());
    assertEquals(1, partitionToBlockIds.get(1).size());
    assertEquals(1, partitionToBlockIds.get(2).size());
    partitionToBlockIds.clear();

    sc.stop();
  }
}
