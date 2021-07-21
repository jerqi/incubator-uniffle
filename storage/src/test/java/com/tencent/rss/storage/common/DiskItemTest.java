package com.tencent.rss.storage.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;


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
      DiskItem item = new DiskItem(testBaseDir.getAbsolutePath(), 50, 100, 100, 100, 5000);
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
      item.getDiskMetaData().setHasRead("app-1/1");
      item.clean();
      assertFalse(dir1.exists());
      assertTrue(dir2.exists());
      item.getDiskMetaData().setHasRead("app-1/2");
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
      DiskItem item = new DiskItem(testBaseDir.getAbsolutePath(), 50, 95, 80, 100, 5000);
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
      DiskItem item = new DiskItem(testBaseDir.getAbsolutePath(), 50, 95, 80, 100, 5000);
      item.updateWrite("1/1", 100);
      item.updateWrite("1/2", 50);
      assertEquals(150L, item.getDiskMetaData().getDiskSize().get());
      item.removeResources("1/1");
      assertEquals(50L, item.getDiskMetaData().getDiskSize().get());
      assertEquals(0L, item.getDiskMetaData().getShuffleSize("1/1"));
      assertEquals(50L, item.getDiskMetaData().getShuffleSize("1/2"));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
