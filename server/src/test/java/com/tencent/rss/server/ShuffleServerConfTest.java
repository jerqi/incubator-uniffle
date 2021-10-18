package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.ExpectedException;

public class ShuffleServerConfTest {

  private static final String confFile = ClassLoader.getSystemResource("confTest.conf").getFile();
  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void defaultConfTest() {
    ShuffleServerConf shuffleServerConf = new ShuffleServerConf();
    assertFalse(shuffleServerConf.loadConfFromFile(null));
    assertEquals("GRPC", shuffleServerConf.getString(ShuffleServerConf.RPC_SERVER_TYPE));
    assertEquals(256, shuffleServerConf.getInteger(ShuffleServerConf.JETTY_CORE_POOL_SIZE));
    assertFalse(shuffleServerConf.getBoolean(ShuffleServerConf.RSS_USE_MULTI_STORAGE));
    assertFalse(shuffleServerConf.getBoolean(ShuffleServerConf.RSS_UPLOADER_ENABLE));
  }

  @Test
  public void envConfTest() {
    environmentVariables.set("RSS_HOME", (new File(confFile)).getParent());
    ShuffleServerConf shuffleServerConf = new ShuffleServerConf(null);
    assertEquals(1234, shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    assertEquals("HDFS", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_TYPE));
    assertEquals("/var/tmp/test", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH));
    environmentVariables.set("RSS_HOME", (new File(confFile)).getParent() + "/wrong_dir/");
    assertFalse(shuffleServerConf.loadConfFromFile(null));
  }

  @Test
  public void confTest() {
    ShuffleServerConf shuffleServerConf = new ShuffleServerConf(confFile);
    assertEquals(1234, shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    assertEquals("FILE", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_TYPE));
    assertEquals("/var/tmp/test", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH));
    assertFalse(shuffleServerConf.loadConfFromFile("/var/tmp/null"));
    assertEquals(2, shuffleServerConf.getLong(ShuffleServerConf.SERVER_BUFFER_CAPACITY));
    assertEquals("value1", shuffleServerConf.getString("rss.server.hadoop.a.b", ""));
    assertEquals("", shuffleServerConf.getString("rss.server.had.a.b", ""));
    assertEquals("COS", shuffleServerConf.getString(ShuffleServerConf.RSS_UPLOAD_STORAGE_TYPE));
    assertEquals("GRPC", shuffleServerConf.getString(ShuffleServerConf.RPC_SERVER_TYPE));
    assertTrue(shuffleServerConf.getBoolean(ShuffleServerConf.RSS_USE_MULTI_STORAGE));
    assertTrue(shuffleServerConf.getBoolean(ShuffleServerConf.RSS_UPLOADER_ENABLE));
    assertEquals(8L, shuffleServerConf.getLong(ShuffleServerConf.RSS_REFERENCE_UPLOAD_SPEED_MBS));
    assertEquals(
        1024L * 1024L * 1024L, shuffleServerConf.getLong(ShuffleServerConf.RSS_SHUFFLE_MAX_UPLOAD_SIZE));
    assertEquals(13, shuffleServerConf.getInteger(ShuffleServerConf.RSS_UPLOADER_THREAD_NUM));

    thrown.expect(NullPointerException.class);
    shuffleServerConf.getInteger(ShuffleServerConf.SERVER_PARTITION_BUFFER_SIZE);

  }
}
