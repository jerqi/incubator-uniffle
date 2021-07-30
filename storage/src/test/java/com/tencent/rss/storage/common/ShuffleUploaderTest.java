package com.tencent.rss.storage.common;

import com.google.common.collect.Lists;
import com.tencent.rss.storage.util.StorageType;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShuffleUploaderTest {

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

      DiskMetaData mockDiskMetaData = mock(DiskMetaData.class);
      when(mockDiskItem.getDiskMetaData())
          .thenReturn(mockDiskMetaData);
      when(mockDiskMetaData.getSortedShuffleKeys(true, 4))
          .thenReturn(Lists.newArrayList(shuffleKey1, "zeroParitionShuffleKey", "zeroSizeShuffleKey"));
      when(mockDiskMetaData.getNotUploadedSize("zeroSizeShuffleKey"))
          .thenReturn(10L);
      when(mockDiskMetaData.getNotUploadedPartitions("zeroSizeShuffleKey"))
          .thenReturn(Lists.newLinkedList());
      when(mockDiskMetaData.getNotUploadedSize("zeroSizeShuffleKey"))
          .thenReturn(0L);
      when(mockDiskMetaData.getNotUploadedPartitions("zeroSizeShuffleKey"))
          .thenReturn(Lists.newArrayList(1));
      when(mockDiskMetaData.getNotUploadedSize(shuffleKey1))
          .thenReturn(30L);
      when(mockDiskMetaData.getNotUploadedPartitions(shuffleKey1))
          .thenReturn(Lists.newArrayList(1, 2, 3));

      List<ShuffleFileInfo> shuffleFileInfos = shuffleUploader.selectShuffleFiles(4);
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
