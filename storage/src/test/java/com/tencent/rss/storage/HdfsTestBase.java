package com.tencent.rss.storage;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class HdfsTestBase implements Serializable {

  @ClassRule
  public static final TemporaryFolder tmpDir = new TemporaryFolder();
  public static Configuration conf;
  protected static String HDFS_URI;
  protected static FileSystem fs;
  protected static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUpHdfs() throws IOException {
    conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
        tmpDir.getRoot().getAbsolutePath());
    cluster = (new MiniDFSCluster.Builder(conf)).build();
    HDFS_URI = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
    fs = (new Path(HDFS_URI)).getFileSystem(conf);
  }

  @AfterClass
  public static void tearDownHdfs() throws IOException {
    fs.close();
    cluster.shutdown();
    tmpDir.delete();
  }

  protected void compareBytes(List<byte[]> expected, List<ByteBuffer> actual) {
    assertEquals(expected.size(), actual.size());

    for (int i = 0; i < expected.size(); i++) {
      byte[] expectedI = expected.get(i);
      ByteBuffer bb = actual.get(i);
      for (int j = 0; j < expectedI.length; j++) {
        assertEquals(expectedI[j], bb.get(j));
      }
    }
  }
}
