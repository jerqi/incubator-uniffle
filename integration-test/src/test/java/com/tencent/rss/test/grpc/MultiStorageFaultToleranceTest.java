package com.tencent.rss.test.grpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MultiStorageFaultToleranceTest extends ShuffleReadWriteBase {
  private ShuffleServerGrpcClient shuffleServerClient;
  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    tmpDir.deleteOnExit();
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE_AND_HDFS.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString(ShuffleServerConf.RSS_HDFS_BASE_PATH,  HDFS_URI + "rss/multi_storage_fault");
    shuffleServerConf.setDouble(ShuffleServerConf.RSS_CLEANUP_THRESHOLD, 0.0);
    shuffleServerConf.setDouble(ShuffleServerConf.RSS_HIGH_WATER_MARK_OF_WRITE, 100.0);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_DISK_CAPACITY, 1024L * 1024L * 100);
    shuffleServerConf.setBoolean(ShuffleServerConf.RSS_UPLOADER_ENABLE, true);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_PENDING_EVENT_TIMEOUT_SEC, 30L);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_UPLOAD_COMBINE_THRESHOLD_MB, 1L);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_SHUFFLE_EXPIRED_TIMEOUT_MS, 5000L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 60L * 1000L * 60L);
    shuffleServerConf.setLong(ShuffleServerConf.SERVER_COMMIT_TIMEOUT, 20L * 1000L);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_PENDING_EVENT_TIMEOUT_SEC, 15);
    shuffleServerConf.setBoolean(ShuffleServerConf.RSS_USE_MULTI_STORAGE, true);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @After
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void hdfsFaultTolerance() {
    try {
      String appId = "app_hdfs_fault_tolerance_data";
      Map<Long, byte[]> expectedData = Maps.newHashMap();

      RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 2,
          Lists.newArrayList(new PartitionRange(0, 0)));
      shuffleServerClient.registerShuffle(rr1);

      RssRegisterShuffleRequest rr2 =  new RssRegisterShuffleRequest(appId, 3,
          Lists.newArrayList(new PartitionRange(1, 1)));
      shuffleServerClient.registerShuffle(rr2);

      RssRegisterShuffleRequest rr3 =  new RssRegisterShuffleRequest(appId, 2,
          Lists.newArrayList(new PartitionRange(2, 2)));
      shuffleServerClient.registerShuffle(rr3);

      Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
      Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
      Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();

      List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
          2, 0, 1,11, 10 * 1024 * 1024, blockIdBitmap1, expectedData);

      List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
          3, 1, 2,9, 10 * 1024 * 1024, blockIdBitmap2, expectedData);

      List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
          2, 2, 2,9, 10 * 1024 * 1024, blockIdBitmap3, expectedData);

      assertEquals(1, cluster.getDataNodes().size());
      cluster.stopDataNode(0);
      assertEquals(0, cluster.getDataNodes().size());

      sendSinglePartitionToShuffleServer(appId, 2, 0, 1, blocks1);

      boolean isException = false;
      try {
        sendSinglePartitionToShuffleServer(appId, 3, 1,2, blocks2);
      } catch (RuntimeException re) {
        isException = true;
        assertTrue(re.getMessage().contains("Fail to finish"));
      }
      assertTrue(isException);

      cluster.startDataNodes(conf, 1, true, HdfsServerConstants.StartupOption.REGULAR,
          null, null, null, false, true);
      assertEquals(1, cluster.getDataNodes().size());

      sendSinglePartitionToShuffleServer(appId, 2, 2, 2, blocks3);

      validateResult(appId, 2, 0, blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1), expectedData);
      validateResult(appId, 2, 2, blockIdBitmap3, Roaring64NavigableMap.bitmapOf(2), expectedData);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void diskFaultTolerance() {
    String appId = "app_disk_fault_tolerance_data";
    Map<Long, byte[]> expectedData = Maps.newHashMap();

    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 2,
        Lists.newArrayList(new PartitionRange(0, 0)));
    shuffleServerClient.registerShuffle(rr1);

    RssRegisterShuffleRequest rr2 =  new RssRegisterShuffleRequest(appId, 3,
        Lists.newArrayList(new PartitionRange(1, 1)));
    shuffleServerClient.registerShuffle(rr2);

    RssRegisterShuffleRequest rr3 =  new RssRegisterShuffleRequest(appId, 2,
        Lists.newArrayList(new PartitionRange(2, 2)));
    shuffleServerClient.registerShuffle(rr3);

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap4 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        2, 0, 1,11, 10 * 1024 * 1024, blockIdBitmap1, expectedData);

    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        3, 1, 2,9, 10 * 1024 * 1024, blockIdBitmap2, expectedData);

    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        2, 2, 2,9, 10 * 1024 * 1024, blockIdBitmap3, expectedData);

    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        2, 0, 1, 11, 10 * 1024 * 1024, blockIdBitmap4, expectedData);
    try {
      sendSinglePartitionToShuffleServer(appId, 2, 0, 1, blocks1);
      sendSinglePartitionToShuffleServer(appId, 3, 1,2, blocks2);
      sendSinglePartitionToShuffleServer(appId, 2, 2, 2, blocks3);
      sendSinglePartitionToShuffleServer(appId, 2, 0, 1, blocks4);
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    validateResult(appId, 2, 0, blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1), expectedData);
    validateResult(appId, 3, 1, blockIdBitmap2, Roaring64NavigableMap.bitmapOf(2), expectedData);
    validateResult(appId, 2, 2, blockIdBitmap3, Roaring64NavigableMap.bitmapOf(2), expectedData);
  }

  private void sendSinglePartitionToShuffleServer(String appId, int shuffle, int partition,
                                                  long taskAttemptId, List<ShuffleBlockInfo> blocks) {
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    Set<Long> expectBlockIds = getExpectBlockIds(blocks);
    partitionToBlocks.put(partition, blocks);
    shuffleToBlocks.put(shuffle, partitionToBlocks);
    RssSendShuffleDataRequest rs = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs);
    RssSendCommitRequest rc = new RssSendCommitRequest(appId, shuffle);
    shuffleServerClient.sendCommit(rc);
    RssFinishShuffleRequest rf = new RssFinishShuffleRequest(appId, shuffle);
    shuffleServerClient.finishShuffle(rf);
    partitionToBlockIds.put(shuffle, new ArrayList<>(expectBlockIds));
    RssReportShuffleResultRequest rrp = new RssReportShuffleResultRequest(appId, shuffle, taskAttemptId, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(rrp);
  }

  protected void validateResult(String appId, int shuffleId, int partitionId, Roaring64NavigableMap blockBitmap,
                                Roaring64NavigableMap taskBitmap, Map<Long, byte[]> expectedData) {
    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl("LOCALFILE_AND_HDFS",
        appId, shuffleId, partitionId, 100, 1, 10, 1000, HDFS_URI + "rss/multi_storage_fault",
        blockBitmap, taskBitmap, Lists.newArrayList(new ShuffleServerInfo("test", LOCALHOST, SHUFFLE_SERVER_PORT)), conf);
    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Roaring64NavigableMap matched = Roaring64NavigableMap.bitmapOf();
    while (csb != null && csb.getByteBuffer() != null) {
      for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.addLong(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertTrue(blockBitmap.equals(matched));
  }


  private Set<Long> getExpectBlockIds(List<ShuffleBlockInfo> blocks) {
    List<Long> expectBlockIds = Lists.newArrayList();
    blocks.forEach(b -> expectBlockIds.add(b.getBlockId()));
    return Sets.newHashSet(expectBlockIds);
  }
}
