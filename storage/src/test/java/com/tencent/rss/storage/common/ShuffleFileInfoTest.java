package com.tencent.rss.storage.common;

import java.io.File;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShuffleFileInfoTest {

  @Test
  public void test() {
    try {
      ShuffleFileInfo shuffleFileInfo = new ShuffleFileInfo();
      shuffleFileInfo.getDataFiles().add(File.createTempFile("dummy-data-file", ".data"));
      shuffleFileInfo.setKey("key");
      assertFalse(shuffleFileInfo.isValid());

      shuffleFileInfo.getIndexFiles().add(File.createTempFile("dummy-data-file", ".index"));
      shuffleFileInfo.getPartitions().add(12);
      shuffleFileInfo.setSize(1024 * 1024 * 32);
      assertTrue(shuffleFileInfo.isValid());
      assertFalse(shuffleFileInfo.shouldCombine(32));
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
