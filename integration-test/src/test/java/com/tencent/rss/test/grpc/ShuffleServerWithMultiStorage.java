package com.tencent.rss.test.grpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.common.DiskItem;
import com.tencent.rss.storage.util.ShuffleStorageUtils;
import com.tencent.rss.storage.util.StorageType;
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShuffleServerWithMultiStorage extends ShuffleReadWriteBase {
  private ShuffleServerGrpcClient shuffleServerClient;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    File tmpDir = Files.createTempDir();
    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString(ShuffleServerConf.RSS_HDFS_BASE_PATH,  HDFS_URI + "rss/multi_storage");
    shuffleServerConf.setDouble(ShuffleServerConf.RSS_CLEANUP_THRESHOLD, 0.0);
    shuffleServerConf.setDouble(ShuffleServerConf.RSS_HIGH_WATER_MARK_OF_WRITE, 100.0);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_DISK_CAPACITY, 1024L * 1024L * 100);
    shuffleServerConf.setBoolean(ShuffleServerConf.RSS_UPLOADER_ENABLE, true);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_PENDING_EVENT_TIMEOUT_SEC, 30L);
    shuffleServerConf.setLong(ShuffleServerConf.RSS_UPLOAD_COMBINE_THRESHOLD_MB, 1L);
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
  public void readUploadedDataTest() {
    String appId = "app_read_uploaded_data";
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(0, 0)));
    RssRegisterShuffleRequest rr2 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(1, 1)));
    RssRegisterShuffleRequest rr3 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(2, 2)));
    RssRegisterShuffleRequest rr4 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(3, 3)));
    shuffleServerClient.registerShuffle(rr1);
    shuffleServerClient.registerShuffle(rr2);
    shuffleServerClient.registerShuffle(rr3);
    shuffleServerClient.registerShuffle(rr4);

    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlock1 = Sets.newHashSet();
    Set<Long> expectedBlock2 = Sets.newHashSet();
    Set<Long> expectedBlock3 = Sets.newHashSet();
    Set<Long> expectedBlock4 = Sets.newHashSet();

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap4 = Roaring64NavigableMap.bitmapOf();

    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        0, 0, 1,3, 25, blockIdBitmap1, expectedData);
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        0, 1, 1,5,1024 * 1024, blockIdBitmap2, expectedData);
    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        0, 2, 2,4, 25, blockIdBitmap3, expectedData);
    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        0, 3, 3,1, 1024 * 1024, blockIdBitmap4, expectedData);

    blocks1.forEach(b -> expectedBlock1.add(b.getBlockId()));
    blocks2.forEach(b -> expectedBlock2.add(b.getBlockId()));
    blocks3.forEach(b -> expectedBlock3.add(b.getBlockId()));
    blocks4.forEach(b -> expectedBlock4.add(b.getBlockId()));

    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    partitionToBlocks.put(1, blocks2);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs1);

    RssSendCommitRequest rc1 = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rc1);
    RssFinishShuffleRequest rf1 = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rf1);
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, new ArrayList<>(expectedBlock1));
    partitionToBlockIds.put(1, new ArrayList<>(expectedBlock2));
    RssReportShuffleResultRequest rrp1 = new RssReportShuffleResultRequest(appId, 0, 1L, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(rrp1);

    partitionToBlocks.clear();
    partitionToBlocks.put(2, blocks3);
    shuffleToBlocks.clear();;
    shuffleToBlocks.put(0, partitionToBlocks);
    partitionToBlockIds.clear();
    RssSendShuffleDataRequest rs2 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs2);
    RssSendCommitRequest rc2 = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rc2);
    RssFinishShuffleRequest rf2 = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rf2);
    partitionToBlockIds.put(2, new ArrayList<>(expectedBlock3));
    RssReportShuffleResultRequest rrp2 = new RssReportShuffleResultRequest(appId, 0, 2L, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(rrp2);

    partitionToBlocks.clear();
    partitionToBlocks.put(3, blocks4);
    shuffleToBlocks.clear();
    shuffleToBlocks.put(0, partitionToBlocks);
    partitionToBlockIds.clear();
    RssSendShuffleDataRequest rs3 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs3);
    RssSendCommitRequest rc3 = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rc3);
    RssFinishShuffleRequest rf3 = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rf3);
    partitionToBlockIds.put(3, new ArrayList<>(expectedBlock4));
    RssReportShuffleResultRequest rrp3 = new RssReportShuffleResultRequest(appId, 0, 3L, partitionToBlockIds);
    shuffleServerClient.reportShuffleResult(rrp3);

    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 0, 0);
    shuffleServerClient.getShuffleResult(rg1);
    RssGetShuffleResultRequest rg2 = new RssGetShuffleResultRequest(appId, 0, 1);
    shuffleServerClient.getShuffleResult(rg2);
    RssGetShuffleResultRequest rg3 = new RssGetShuffleResultRequest(appId, 0, 2);
    shuffleServerClient.getShuffleResult(rg3);
    RssGetShuffleResultRequest rg4 = new RssGetShuffleResultRequest(appId, 0, 3);
    shuffleServerClient.getShuffleResult(rg4);

    do {
      try {
        RssAppHeartBeatRequest ra = new RssAppHeartBeatRequest(appId, 1000);
        shuffleServerClient.sendHeartBeat(ra);
        boolean uploadFinished = true;
        for (int i = 0; i < 4; i++) {
          DiskItem diskItem = shuffleServers.get(0).getMultiStorageManager().getDiskItem(appId, 0, 0);
          String path = ShuffleStorageUtils.getFullShuffleDataFolder(diskItem.getBasePath(),
              ShuffleStorageUtils.getShuffleDataPath(appId, 0, i, i));
          File file = new File(path);
          if (file.exists()) {
            uploadFinished = false;
            break;
          }
        }
        if (uploadFinished) {
          break;
        }
        Thread.sleep(1000);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    } while(true);

     ShuffleReadClientImpl readClient = new ShuffleReadClientImpl("HDFS_BACKUP",
        appId, 0, 0, 100, 1, 10, 1000, HDFS_URI + "rss/multi_storage",
        blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1), Lists.newArrayList(), conf);
     validateResult(readClient, expectedData, blockIdBitmap1);

     readClient = new ShuffleReadClientImpl("HDFS_BACKUP",
        appId, 0, 1, 100, 1, 10, 1000, HDFS_URI + "rss/multi_storage",
        blockIdBitmap2, Roaring64NavigableMap.bitmapOf(1), Lists.newArrayList(), conf);
    validateResult(readClient, expectedData, blockIdBitmap2);

    readClient = new ShuffleReadClientImpl("HDFS_BACKUP",
        appId, 0, 2, 100, 1, 10, 1000, HDFS_URI + "rss/multi_storage",
        blockIdBitmap3, Roaring64NavigableMap.bitmapOf(2), Lists.newArrayList(), conf);
    validateResult(readClient, expectedData, blockIdBitmap3);

    readClient = new ShuffleReadClientImpl("HDFS_BACKUP",
        appId, 0, 3, 100, 1, 10, 1000, HDFS_URI + "rss/multi_storage",
        blockIdBitmap4, Roaring64NavigableMap.bitmapOf(3), Lists.newArrayList(), conf);
    validateResult(readClient, expectedData, blockIdBitmap4);
  }

  @Test
  public void readLocalDataTest() {

  }

  @Test
  public void diskUsageTest() {

  }

  protected void validateResult(ShuffleReadClientImpl readClient, Map<Long, byte[]> expectedData,
      Roaring64NavigableMap blockIdBitmap) {
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
    assertTrue(blockIdBitmap.equals(matched));
  }
}
