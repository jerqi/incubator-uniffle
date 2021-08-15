package com.tencent.rss.storage.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.roaringbitmap.RoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class DiskItemTest {

  @ClassRule
  public static final TemporaryFolder tmpDir = new TemporaryFolder();
  private static File testBaseDir;

  @BeforeClass
  public static void setUp() throws IOException  {
    testBaseDir = tmpDir.newFolder("test");
  }

  @AfterClass
  public static void tearDown() {
    tmpDir.delete();
  }

  @Test
  public void cleanTest() {
    try {
      DiskItem item = DiskItem.newBuilder().basePath(testBaseDir.getAbsolutePath())
          .cleanupThreshold(50)
          .highWaterMarkOfWrite(100)
          .lowWaterMarkOfWrite(100)
          .capacity(100)
          .cleanIntervalMs(5000)
          .shuffleExpiredTimeoutMs(1)
          .build();
      File baseDir = tmpDir.newFolder(testBaseDir.getName(),"app-1");
      assertTrue(baseDir.exists());
      File dir1 = tmpDir.newFolder(testBaseDir.getName(), "app-1", "1");
      File dir2 = tmpDir.newFolder(testBaseDir.getName(), "app-1", "2");
      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      item.clean();
      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      item.getDiskMetaData().updateShuffleSize("app-1/1", 25);
      item.getDiskMetaData().updateShuffleSize("app-1/2", 35);
      item.getDiskMetaData().updateDiskSize(60);
      assertEquals(60, item.getDiskMetaData().getDiskSize().get());
      item.clean();
      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      item.getDiskMetaData().updateUploadedShuffleSize("app-1/1", 25);
      item.clean();
      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      item.getDiskMetaData().prepareStartRead("app-1/1");
      item.clean();
      assertTrue(dir1.exists());
      item.getDiskMetaData().updateShuffleLastReadTs("app-1/1");
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      item.clean();
      assertFalse(dir1.exists());
      assertTrue(dir2.exists());
      item.getDiskMetaData().prepareStartRead("app-1/2");
      item.clean();
      assertTrue(dir2.exists());
      assertEquals(35, item.getDiskMetaData().getDiskSize().get());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void canWriteTest() {
    try {
      DiskItem item = DiskItem.newBuilder().basePath(testBaseDir.getAbsolutePath())
          .cleanupThreshold(50)
          .highWaterMarkOfWrite(95)
          .lowWaterMarkOfWrite(80)
          .capacity(100)
          .cleanIntervalMs(5000)
          .build();

      item.getDiskMetaData().updateDiskSize(20);
      assertTrue(item.canWrite());
      item.getDiskMetaData().updateDiskSize(65);
      assertTrue(item.canWrite());
      item.getDiskMetaData().updateDiskSize(10);
      assertFalse(item.canWrite());
      item.getDiskMetaData().updateDiskSize(-10);
      assertFalse(item.canWrite());
      item.getDiskMetaData().updateDiskSize(-10);
      assertTrue(item.canWrite());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void removeResourcesTest() {
    try {
      DiskItem item = DiskItem.newBuilder().basePath(testBaseDir.getAbsolutePath())
          .cleanupThreshold(50)
          .highWaterMarkOfWrite(95)
          .lowWaterMarkOfWrite(80)
          .capacity(100)
          .cleanIntervalMs(5000)
          .build();
      RoaringBitmap partitionBitMap = RoaringBitmap.bitmapOf();
      partitionBitMap.add(1);
      partitionBitMap.add(2);
      partitionBitMap.add(1);
      List<Integer> partitionList = Lists.newArrayList(1, 2);
      item.updateWrite("1/1", 100, partitionList);
      item.updateWrite("1/2", 50, Lists.newArrayList());
      assertEquals(150L, item.getDiskMetaData().getDiskSize().get());
      assertEquals(2, item.getDiskMetaData().getNotUploadedPartitions("1/1").getCardinality());
      assertTrue(partitionBitMap.contains(item.getDiskMetaData().getNotUploadedPartitions("1/1")));
      item.removeResources("1/1");
      assertEquals(50L, item.getDiskMetaData().getDiskSize().get());
      assertEquals(0L, item.getDiskMetaData().getShuffleSize("1/1"));
      assertEquals(50L, item.getDiskMetaData().getShuffleSize("1/2"));
      assertEquals(0, item.getDiskMetaData().getNotUploadedPartitions("1/1").getCardinality());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void diskMetaTest() {
    DiskItem item = DiskItem.newBuilder().basePath(testBaseDir.getAbsolutePath())
        .cleanupThreshold(50)
        .highWaterMarkOfWrite(95)
        .lowWaterMarkOfWrite(80)
        .capacity(100)
        .cleanIntervalMs(5000)
        .build();
    List<Integer> partitionList1 = Lists.newArrayList(1, 2, 3, 4, 5);
    List<Integer> partitionList2 = Lists.newArrayList(6, 7, 8, 9, 10);
    List<Integer> partitionList3 = Lists.newArrayList(1, 2, 3);
    item.updateWrite("key1", 10, partitionList1);
    item.updateWrite("key2", 30, partitionList2);
    item.updateUploadedShuffle("key1", 5, partitionList3);

    assertTrue(item.getNotUploadedPartitions("notKey").isEmpty());
    assertEquals(2, item.getNotUploadedPartitions("key1").getCardinality());
    assertEquals(5, item.getNotUploadedPartitions("key2").getCardinality());
    assertEquals(0, item.getNotUploadedSize("notKey"));
    assertEquals(5, item.getNotUploadedSize("key1"));
    assertEquals(30, item.getNotUploadedSize("key2"));

    assertTrue(item.getSortedShuffleKeys(true, 1).isEmpty());
    assertTrue(item.getSortedShuffleKeys(true, 2).isEmpty());
    item.prepareStartRead("key1");
    assertEquals(1, item.getSortedShuffleKeys(true, 3).size());
    assertEquals(1, item.getSortedShuffleKeys(false, 1).size());
    assertEquals("key2", item.getSortedShuffleKeys(false, 1).get(0));
    assertEquals(2, item.getSortedShuffleKeys(false, 2).size());
    assertEquals(2, item.getSortedShuffleKeys(false, 3).size());
  }
}
