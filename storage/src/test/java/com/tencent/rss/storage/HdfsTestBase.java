package com.tencent.rss.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class HdfsTestBase {

  @ClassRule
  public static final TemporaryFolder tmpDir = new TemporaryFolder();
  protected static Configuration conf;
  protected static String HDFS_URI;
  protected static FileSystem fs;
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUp() throws IOException {
    conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
      tmpDir.getRoot().getAbsolutePath());
    cluster = (new MiniDFSCluster.Builder(conf)).build();
    HDFS_URI = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
    fs = (new Path(HDFS_URI)).getFileSystem(conf);
  }


  @AfterClass
  public static void tearDown() throws IOException {
    fs.close();
    cluster.shutdown();
  }

}
