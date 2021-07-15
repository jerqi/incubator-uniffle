package com.tencent.rss.storage.common;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class DiskItemTest {
  @Test
  public void cleanTest() {
    try {
      TemporaryFolder tmpDir = new TemporaryFolder();
      tmpDir.create();
      DiskItem item = new DiskItem(100, tmpDir.getRoot().getAbsolutePath(), 50, 100, 100);
      File baseDir = tmpDir.newFolder("app-1");
      Assert.assertTrue(baseDir.exists());
      File dir1 = tmpDir.newFolder("app-1", "1");
      File dir2 = tmpDir.newFolder("app-1", "2");
      Assert.assertTrue(dir1.exists());
      Assert.assertTrue(dir2.exists());
      item.clean();
      Assert.assertTrue(dir1.exists());
      Assert.assertTrue(dir2.exists());
      item.getDiskMetaData().updateShuffleSize("app-1/1", 25);
      item.getDiskMetaData().updateShuffleSize("app-1/2", 35);
      item.getDiskMetaData().updateDiskSize(60);
      Assert.assertEquals(60, item.getDiskMetaData().getDiskSize().get());
      item.clean();
      Assert.assertTrue(dir1.exists());
      Assert.assertTrue(dir2.exists());
      item.getDiskMetaData().updateUploadedShuffleSize("app-1/1", 25);
      item.clean();
      Assert.assertTrue(dir1.exists());
      Assert.assertTrue(dir2.exists());
      item.getDiskMetaData().setHasRead("app-1/1");
      item.clean();
      Assert.assertFalse(dir1.exists());
      Assert.assertTrue(dir2.exists());
      item.getDiskMetaData().setHasRead("app-1/2");
      item.clean();
      Assert.assertTrue(dir2.exists());
      Assert.assertEquals(35, item.getDiskMetaData().getDiskSize().get());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
