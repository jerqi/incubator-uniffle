package com.tencent.rss.server;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.storage.common.DiskItem;
import com.tencent.rss.storage.common.ShuffleFileInfo;
import com.tencent.rss.storage.factory.ShuffleUploadHandlerFactory;
import com.tencent.rss.storage.handler.api.ShuffleUploadHandler;
import com.tencent.rss.storage.util.ShuffleUploadResult;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public class ShuffleUploaderTest  {

  @ClassRule
  public static final TemporaryFolder tmpDir = new TemporaryFolder();
  private static File base;

  @BeforeClass
  public static void setUp() throws IOException {
    base = tmpDir.newFolder("ShuffleUploaderTest");
  }

  @AfterClass
  public static void tearDown() {
    tmpDir.delete();
  }


  @Test
  public void builderTest() {
    DiskItem mockDiskItem = mock(DiskItem.class);
    when(mockDiskItem.getBasePath()).thenReturn("test/base");
    assertException(
        IllegalArgumentException.class,
        (Void) -> new ShuffleUploader.Builder().diskItem(mockDiskItem).build());

    assertException(
        IllegalArgumentException.class,
        (Void) -> new ShuffleUploader.Builder().diskItem(mockDiskItem).uploadThreadNum(2).build());

    assertException(
        IllegalArgumentException.class,
        (Void) -> new ShuffleUploader.Builder().diskItem(mockDiskItem).uploadThreadNum(2).uploadIntervalMS(3).build());

    assertException(
        IllegalArgumentException.class,
        (Void) -> new ShuffleUploader.Builder()
            .diskItem(mockDiskItem).uploadThreadNum(2).uploadIntervalMS(3).uploadCombineThresholdMB(300).build());

    assertException(
        IllegalArgumentException.class,
        (Void) -> new ShuffleUploader.Builder()
            .diskItem(mockDiskItem)
            .uploadThreadNum(2)
            .uploadIntervalMS(3)
            .uploadCombineThresholdMB(300)
            .referenceUploadSpeedMBS(1)
            .build());

    assertException(
        IllegalArgumentException.class,
        (Void) -> new ShuffleUploader.Builder()
            .diskItem(mockDiskItem)
            .uploadThreadNum(2)
            .uploadIntervalMS(3)
            .uploadCombineThresholdMB(300)
            .referenceUploadSpeedMBS(1)
            .remoteStorageType(null)
            .build());

    assertException(
        IllegalArgumentException.class,
        (Void) -> new ShuffleUploader.Builder()
            .diskItem(mockDiskItem)
            .uploadThreadNum(2)
            .uploadIntervalMS(3)
            .uploadCombineThresholdMB(300)
            .referenceUploadSpeedMBS(1)
            .hdfsBathPath("hdfs://base")
            .serverId("")
            .hadoopConf(new Configuration())
            .build());

    new ShuffleUploader.Builder()
        .diskItem(mockDiskItem)
        .uploadThreadNum(2)
        .uploadIntervalMS(3)
        .uploadCombineThresholdMB(300)
        .referenceUploadSpeedMBS(1)
        .remoteStorageType(StorageType.HDFS)
        .hdfsBathPath("hdfs://base")
        .serverId("prefix")
        .hadoopConf(new Configuration())
        .build();
  }

  @Test
  public void selectShuffleFiles() {
    try {
      String app1 = "app-1";
      String shuffle1 = "1";
      String shuffleKey1 = String.join("/", app1, shuffle1);
      File partitionDir1 = tmpDir.newFolder(base.getName(), app1, shuffle1, "1-1");
      File partitionDir2 = tmpDir.newFolder(base.getName(), app1, shuffle1, "2-2");
      File partitionDir3 = tmpDir.newFolder(base.getName(), app1, shuffle1, "3-3");
      File partitionDir4 = tmpDir.newFolder(base.getName(), app1, shuffle1, "4-4");
      File partitionDir5 = tmpDir.newFolder(base.getName(), app1, shuffle1, "5-5");

      File dataFile1 = new File(partitionDir1.getAbsolutePath() + "/127.0.0.1-8080.data");
      File dataFile2 = new File(partitionDir2.getAbsolutePath() + "/127.0.0.1-8080.data");
      File dataFile3 = new File(partitionDir3.getAbsolutePath() + "/127.0.0.1-8080.data");
      File dataFile4 = new File(partitionDir5.getAbsolutePath() + "/127.0.0.1-8080.data");

      File indexFile1 = new File(partitionDir1.getAbsolutePath() + "/127.0.0.1-8080.index");
      File indexFile2 = new File(partitionDir2.getAbsolutePath() + "/127.0.0.1-8080.index");
      File indexFile3 = new File(partitionDir3.getAbsolutePath() + "/127.0.0.1-8080.index");
      File indexFile5 = new File(partitionDir4.getAbsolutePath() + "/127.0.0.1-8080.index");

      List<File> dataFiles = Lists.newArrayList(dataFile1, dataFile2, dataFile3, dataFile4);
      dataFiles.forEach(f -> {
        byte[] data1 = new byte[10];
        new Random().nextBytes(data1);
        try (OutputStream out = new FileOutputStream(f)) {
          out.write(data1);
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });

      List<File> indexFiles = Lists.newArrayList(indexFile1, indexFile2, indexFile3, indexFile5);
      indexFiles.forEach(f -> {
        byte[] data1 = new byte[10];
        new Random().nextBytes(data1);
        try (OutputStream out = new FileOutputStream(f)) {
          out.write(data1);
        } catch (IOException e) {
          fail(e.getMessage());
        }
      });

      DiskItem mockDiskItem = mock(DiskItem.class);
      when(mockDiskItem.getBasePath()).thenReturn(base.getAbsolutePath());
      ShuffleUploader shuffleUploader = new ShuffleUploader.Builder()
          .diskItem(mockDiskItem)
          .uploadThreadNum(2)
          .uploadIntervalMS(3)
          .uploadCombineThresholdMB(300)
          .referenceUploadSpeedMBS(1)
          .remoteStorageType(StorageType.HDFS)
          .hdfsBathPath("hdfs://base")
          .serverId("127.0.0.1-8080")
          .hadoopConf(new Configuration())
          .build();

      when(mockDiskItem.getSortedShuffleKeys(true, 4))
          .thenReturn(Lists.newArrayList(shuffleKey1, "zeroPartitionShuffleKey", "zeroSizeShuffleKey"));
      when(mockDiskItem.getNotUploadedSize("zeroSizeShuffleKey"))
          .thenReturn(10L);
      when(mockDiskItem.getNotUploadedPartitions("zeroSizeShuffleKey"))
          .thenReturn(RoaringBitmap.bitmapOf());
      when(mockDiskItem.getNotUploadedSize("zeroSizeShuffleKey"))
          .thenReturn(0L);
      when(mockDiskItem.getNotUploadedPartitions("zeroSizeShuffleKey"))
          .thenReturn(RoaringBitmap.bitmapOf(1));
      when(mockDiskItem.getNotUploadedSize(shuffleKey1))
          .thenReturn(30L);
      when(mockDiskItem.getNotUploadedPartitions(shuffleKey1))
          .thenReturn(RoaringBitmap.bitmapOf(1, 2, 3));
      when(mockDiskItem.getNotUploadedPartitions("zeroPartitionShuffleKey"))
          .thenReturn(RoaringBitmap.bitmapOf());

      List<ShuffleFileInfo> shuffleFileInfos = shuffleUploader.selectShuffleFiles(4, false);
      assertEquals(1, shuffleFileInfos.size());
      ShuffleFileInfo shuffleFileInfo = shuffleFileInfos.get(0);
      for (int i = 0; i < 3; ++i) {
        assertEquals(dataFiles.get(i).getAbsolutePath(), shuffleFileInfo.getDataFiles().get(i).getAbsolutePath());
        assertEquals(indexFiles.get(i).getAbsolutePath(), shuffleFileInfo.getIndexFiles().get(i).getAbsolutePath());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void calculateUploadTimeTest() {
    DiskItem mockDiskItem = mock(DiskItem.class);
    when(mockDiskItem.getBasePath()).thenReturn(base.getAbsolutePath());
    ShuffleUploader shuffleUploader = new ShuffleUploader.Builder()
        .diskItem(mockDiskItem)
        .uploadThreadNum(2)
        .uploadIntervalMS(3)
        .uploadCombineThresholdMB(300)
        .referenceUploadSpeedMBS(128)
        .remoteStorageType(StorageType.HDFS)
        .hdfsBathPath("hdfs://base")
        .serverId("prefix")
        .hadoopConf(new Configuration())
        .build();
    assertEquals(2, shuffleUploader.calculateUploadTime(0));
    assertEquals(2, shuffleUploader.calculateUploadTime(128 * 1024));
    assertEquals(2, shuffleUploader.calculateUploadTime(128 * 1024 * 1024));
    assertEquals(6, shuffleUploader.calculateUploadTime(3 * 128 * 1024 * 1024));
  }

  @Test
  public void uploadTest() {
    try {
      ShuffleUploader.Builder builder = new ShuffleUploader.Builder();
      DiskItem diskItem = DiskItem.newBuilder()
          .capacity(100)
          .basePath(base.getAbsolutePath())
          .highWaterMarkOfWrite(50)
          .lowWaterMarkOfWrite(45)
          .shuffleExpiredTimeoutMs(1000)
          .build();
      builder.diskItem(diskItem);
      builder.hadoopConf(new Configuration());
      builder.hdfsBathPath("hdfs://test");
      builder.referenceUploadSpeedMBS(2);
      builder.remoteStorageType(StorageType.HDFS);
      builder.serverId("test");
      builder.uploadCombineThresholdMB(1);
      builder.uploadThreadNum(1);
      builder.uploadIntervalMS(1000);
      ShuffleUploadHandlerFactory mockFactory = mock(ShuffleUploadHandlerFactory.class);
      ShuffleUploadHandler mockHandler = mock(ShuffleUploadHandler.class);
      when(mockFactory.createShuffleUploadHandler(any())).thenReturn(mockHandler);
      ShuffleUploadResult result0 = new ShuffleUploadResult(50, Lists.newArrayList(1, 2));
      ShuffleUploadResult result1 = new ShuffleUploadResult(90, Lists.newArrayList(1, 2, 3));
      ShuffleUploadResult result2 = new ShuffleUploadResult(10, Lists.newArrayList(1, 2));
      ShuffleUploadResult result3 = new ShuffleUploadResult(40, Lists.newArrayList(1, 3, 2, 4));
      when(mockHandler.upload(any(),any(), any())).thenReturn(result0).thenReturn(result1)
          .thenReturn(result2).thenReturn(result3);

      ShuffleUploader uploader = spy(builder.build());
      when(uploader.getHandlerFactory()).thenReturn(mockFactory);
      diskItem.updateWrite("key", 70, Lists.newArrayList(1, 2, 3));
      File dir1 = new File(base.getAbsolutePath() + "/key/1-1/");
      dir1.mkdirs();
      File file1d = new File(base.getAbsolutePath() + "/key/1-1/test.data");
      file1d.createNewFile();
      File file1i = new File(base.getAbsolutePath() + "/key/1-1/test.index");
      file1i.createNewFile();
      File dir2 = new File(base.getAbsolutePath() + "/key/2-2/");
      dir2.mkdirs();
      File file2d = new File(base.getAbsolutePath() + "/key/2-2/test.data");
      file2d.createNewFile();
      File file2i = new File(base.getAbsolutePath() + "/key/2-2/test.index");
      file2i.createNewFile();
      File dir3 = new File(base.getAbsolutePath() + "/key/3-3/");
      dir3.mkdirs();
      File file3d = new File(base.getAbsolutePath() + "/key/3-3/test.data");
      file3d.createNewFile();
      File file3i = new File(base.getAbsolutePath() + "/key/3-3/test.index");
      file3i.createNewFile();
      uploader.upload();
      assertEquals(20, diskItem.getNotUploadedSize("key"));
      assertEquals(1, diskItem.getNotUploadedPartitions("key").getCardinality());
      assertTrue(diskItem.getNotUploadedPartitions("key").contains(3));
      assertFalse(file1d.exists());
      assertFalse(file1i.exists());
      assertFalse(file2d.exists());
      assertFalse(file2i.exists());
      assertTrue(file3d.exists());
      assertTrue(file3i.exists());

      diskItem.updateWrite("key", 70, Lists.newArrayList(1, 2));
      file1d.createNewFile();
      file1i.createNewFile();
      file2d.createNewFile();
      file2i.createNewFile();
      uploader.upload();
      assertEquals(0, diskItem.getNotUploadedSize("key"));
      assertTrue(diskItem.getNotUploadedPartitions("key").isEmpty());
      assertFalse(file1d.exists());
      assertFalse(file1i.exists());
      assertFalse(file2d.exists());
      assertFalse(file2i.exists());
      assertFalse(file3d.exists());
      assertFalse(file3i.exists());

      diskItem.updateWrite("key", 30, Lists.newArrayList(1, 2, 3));
      file1d.createNewFile();
      file1i.createNewFile();
      file2d.createNewFile();
      file2i.createNewFile();
      file3i.createNewFile();
      file3d.createNewFile();
      uploader.upload();
      assertEquals(30, diskItem.getNotUploadedSize("key"));
      assertEquals(3, diskItem.getNotUploadedPartitions("key").getCardinality());

      diskItem.prepareStartRead("key");
      uploader.upload();
      assertEquals(20, diskItem.getNotUploadedSize("key"));
      assertEquals(1, diskItem.getNotUploadedPartitions("key").getCardinality());
      assertTrue(file1d.exists());
      assertTrue(file1i.exists());
      assertTrue(file2d.exists());
      assertTrue(file2i.exists());
      assertTrue(file3d.exists());
      assertTrue(file3i.exists());

      diskItem.updateShuffleLastReadTs("key");
      diskItem.start();
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
      assertTrue(file1d.exists());
      assertTrue(file1i.exists());
      assertTrue(file2d.exists());
      assertTrue(file2i.exists());
      assertTrue(file3d.exists());
      assertTrue(file3i.exists());

      diskItem.updateShuffleLastReadTs("key");
      diskItem.updateWrite("key", 20, Lists.newArrayList(1, 2, 4));
      uploader.upload();
      assertEquals(0, diskItem.getNotUploadedSize("key"));
      assertTrue(diskItem.getNotUploadedPartitions("key").isEmpty());
      Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

      assertFalse(file1d.exists());
      assertFalse(file1i.exists());
      assertFalse(file2d.exists());
      assertFalse(file2i.exists());
      assertFalse(file3d.exists());
      assertFalse(file3i.exists());
      diskItem.stop();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private void assertException(Class<?> c, Consumer<Void> f) {
    BiConsumer<Class<?>, Consumer<Void>> checker = (expectedExceptionClass, func) -> {
      try {
        func.accept(null);
      } catch (Exception e) {
        assertEquals(expectedExceptionClass, e.getClass());
      }
    };
    checker.accept(c, f);
  }

}
