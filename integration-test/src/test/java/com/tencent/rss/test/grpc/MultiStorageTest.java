package com.tencent.rss.test.grpc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.*;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.util.ChecksumUtils;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MultiStorageTest extends ShuffleReadWriteBase {
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
    shuffleServerConf.setString("rss.storage.type", StorageType.LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setString(ShuffleServerConf.RSS_HDFS_BASE_PATH,  HDFS_URI + "rss/multi_storage");
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

    sendSinglePartitionToShuffleServer(appId, 0,2, 2L, blocks3);
    sendSinglePartitionToShuffleServer(appId, 0, 3, 3L, blocks4);

    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 0, 0);
    shuffleServerClient.getShuffleResult(rg1);
    RssGetShuffleResultRequest rg2 = new RssGetShuffleResultRequest(appId, 0, 1);
    shuffleServerClient.getShuffleResult(rg2);
    RssGetShuffleResultRequest rg3 = new RssGetShuffleResultRequest(appId, 0, 2);
    shuffleServerClient.getShuffleResult(rg3);
    RssGetShuffleResultRequest rg4 = new RssGetShuffleResultRequest(appId, 0, 3);
    shuffleServerClient.getShuffleResult(rg4);

    RssGetShuffleDataRequest rd1 = new RssGetShuffleDataRequest(appId, 0, 0, 1, 10, 100, 0);
    shuffleServerClient.getShuffleData(rd1);
    RssGetShuffleDataRequest rd2 = new RssGetShuffleDataRequest(appId, 0, 1, 1, 10, 100, 0);
    shuffleServerClient.getShuffleData(rd2);

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

    RssGetShuffleDataRequest req = new RssGetShuffleDataRequest(appId, 0, 0,
        1, 10, 1000,  0);
    boolean isException = false;
    try {
      RssGetShuffleDataResponse res = shuffleServerClient.getShuffleData(req);
      ShuffleDataResult result = res.getShuffleDataResult();
      result = res.getShuffleDataResult();
    } catch (RuntimeException re) {
      isException = true;
      assertTrue(re.getMessage().contains("Can't get shuffle data"));
    }
    assertTrue(isException);


    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl("LOCALFILE",
        appId, 0, 0, 100, 1, 10, 1000, HDFS_URI + "rss/multi_storage",
        blockIdBitmap1, Roaring64NavigableMap.bitmapOf(1), Lists.newArrayList(), conf);

    CompressedShuffleBlock result = readClient.readShuffleBlockData();
    assertNull(result);
    // RssShuffleDataIterator iterator = new RssShuffleDataIterator();
    readClient = new ShuffleReadClientImpl("HDFS_BACKUP",
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
    String appId = "app_read_not_uploaded_data";
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 1,
        Lists.newArrayList(new PartitionRange(0, 0)));
    RssRegisterShuffleRequest rr2 =  new RssRegisterShuffleRequest(appId, 1,
        Lists.newArrayList(new PartitionRange(1, 1)));
    RssRegisterShuffleRequest rr3 =  new RssRegisterShuffleRequest(appId, 1,
        Lists.newArrayList(new PartitionRange(2, 2)));
    RssRegisterShuffleRequest rr4 =  new RssRegisterShuffleRequest(appId, 1,
        Lists.newArrayList(new PartitionRange(3, 3)));
    shuffleServerClient.registerShuffle(rr1);
    shuffleServerClient.registerShuffle(rr2);
    shuffleServerClient.registerShuffle(rr3);
    shuffleServerClient.registerShuffle(rr4);

    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap3 = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap blockIdBitmap4 = Roaring64NavigableMap.bitmapOf();


    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        1, 0, 1,3, 25, blockIdBitmap1, expectedData);
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        1, 1, 2,5,1024 * 1024, blockIdBitmap2, expectedData);
    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
        1, 2, 3,4, 25, blockIdBitmap3, expectedData);
    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
        1, 3, 4,1, 1024 * 1024, blockIdBitmap4, expectedData);

    sendSinglePartitionToShuffleServer(appId, 1,0, 1L, blocks1);
    sendSinglePartitionToShuffleServer(appId, 1,1, 2L, blocks2);
    sendSinglePartitionToShuffleServer(appId, 1,2, 3L, blocks3);
    sendSinglePartitionToShuffleServer(appId, 1,3, 4L, blocks4);

    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 1, 0);
    shuffleServerClient.getShuffleResult(rg1);
    RssGetShuffleResultRequest rg2 = new RssGetShuffleResultRequest(appId, 1, 1);
    shuffleServerClient.getShuffleResult(rg2);
    RssGetShuffleResultRequest rg3 = new RssGetShuffleResultRequest(appId, 1, 2);
    shuffleServerClient.getShuffleResult(rg3);
    RssGetShuffleResultRequest rg4 = new RssGetShuffleResultRequest(appId, 1, 3);
    shuffleServerClient.getShuffleResult(rg4);

    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    validateResult(appId, 1, 0, expectedData, getExpectBlockIds(blocks1));
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    validateResult(appId, 1, 1, expectedData, getExpectBlockIds(blocks2));
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    validateResult(appId, 1, 2, expectedData, getExpectBlockIds(blocks3));
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    validateResult(appId, 1, 3, expectedData, getExpectBlockIds(blocks4));
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    RssGetShuffleDataRequest req = new RssGetShuffleDataRequest(appId, 1, 0,
        1, 10, 1000,  0);
    boolean isException = false;
    try {
      RssGetShuffleDataResponse res = shuffleServerClient.getShuffleData(req);
      res.getShuffleDataResult();
    } catch (RuntimeException re) {
      isException = true;
      assertTrue(re.getMessage().contains("Can't get shuffle data"));
    }
    assertTrue(isException);
  }

  @Test
  public void diskUsageTest() {
    String appId = "app_read_diskusage_data";
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
    sendSinglePartitionToShuffleServer(appId, 2, 0, 1, blocks1);
    boolean isException = false;
    try {
      sendSinglePartitionToShuffleServer(appId, 2, 2, 2, blocks3);
    } catch (RuntimeException re) {
      isException = true;
      assertTrue(re.getMessage().contains("Can't finish shuffle process"));
    }
    assertTrue(isException);
    RssGetShuffleResultRequest rg1 = new RssGetShuffleResultRequest(appId, 2, 0);
    shuffleServerClient.getShuffleResult(rg1);
    validateResult(appId, 2, 0, expectedData, getExpectBlockIds(blocks1));
    try {
      sendSinglePartitionToShuffleServer(appId, 3, 1,2, blocks2);
    } catch (RuntimeException re) {
      fail();
    }
    RssGetShuffleResultRequest rg2 = new RssGetShuffleResultRequest(appId, 3, 1);
    shuffleServerClient.getShuffleResult(rg2);
    validateResult(appId, 3, 1, expectedData,
        getExpectBlockIds(blocks2));

    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
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

  protected void validateResult(String appId, int shuffleId, int partitionId,
     Map<Long, byte[]> expectedData, Set<Long> expectBlockIds) {
    int matched = 0;
    int index = 0;
    ShuffleDataResult result = null;
    do {
      if (result != null) {
        byte[] buffer = result.getData();
        for (BufferSegment bs : result.getBufferSegments()) {
          if (expectBlockIds.contains(bs.getBlockId())) {
            byte[] data = new byte[bs.getLength()];
            System.arraycopy(buffer, bs.getOffset(), data, 0, bs.getLength());
            assertEquals(bs.getCrc(), ChecksumUtils.getCrc32(data));
            assertTrue(Arrays.equals(data, expectedData.get(bs.getBlockId())));
            matched++;
          }
          assertTrue(expectBlockIds.contains(bs.getBlockId()));
        }
      }
      RssGetShuffleDataRequest req = new RssGetShuffleDataRequest(appId, shuffleId, partitionId,
        1, 10, 1000, index);
      RssGetShuffleDataResponse res = shuffleServerClient.getShuffleData(req);
      result = res.getShuffleDataResult();
      ++index;
    } while(result != null && result.getData() != null
      && result.getBufferSegments() != null && !result.getBufferSegments().isEmpty());
    assertEquals(expectBlockIds.size(), matched);
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

  private Set<Long> getExpectBlockIds(List<ShuffleBlockInfo> blocks) {
    List<Long> expectBlockIds = Lists.newArrayList();
    blocks.forEach(b -> expectBlockIds.add(b.getBlockId()));
    return Sets.newHashSet(expectBlockIds);
  }
}
