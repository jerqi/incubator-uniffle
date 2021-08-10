package com.tencent.rss.storage.util;

import com.tencent.rss.storage.HdfsTestBase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ShuffleHdfsStorageUtilsTest extends HdfsTestBase {

  @Test
  public void testUploadFile() {
    FSDataOutputStream out = null;
    FileOutputStream fileOut = null;
    DataOutputStream dataOut = null;
    try {
      TemporaryFolder tmpDir = new TemporaryFolder();
      tmpDir.create();
      File file = tmpDir.newFile("test");
      fileOut = new FileOutputStream(file);
      dataOut = new DataOutputStream(fileOut);
      byte[] buf = new byte[2096];
      new Random().nextBytes(buf);
      dataOut.write(buf);
      dataOut.close();
      fileOut.close();
      String path = HDFS_URI + "test";
      FileSystem fs = FileSystem.get(conf);
      out = fs.create(new Path(path));
      long size = ShuffleStorageUtils.uploadFile(file, out, 1024);
      assertEquals(2096, size);
      size = ShuffleStorageUtils.uploadFile(file, out, 100);
      assertEquals(2096, size);
      tmpDir.delete();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
