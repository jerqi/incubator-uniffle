package com.tencent.rss.storage.util;

import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.handler.impl.HdfsFileWriter;
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
      HdfsFileWriter writer = new HdfsFileWriter(new Path(path), conf);
      long size = ShuffleStorageUtils.uploadFile(file, writer, 1024);
      assertEquals(2096, size);
      size = ShuffleStorageUtils.uploadFile(file, writer, 100);
      assertEquals(2096, size);
      writer.close();
      tmpDir.delete();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }
}
