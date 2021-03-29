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
  private ShuffleServerConf shuffleServerConf;

  @Before
  public void setUp() {
    shuffleServerConf = new ShuffleServerConf(confFile);
  }

  @After
  public void tearDown() {
    shuffleServerConf = null;
  }

  @Test
  public void defaultConfTest() {
    assertFalse(shuffleServerConf.loadConfFromFile(null));
  }

  @Test
  public void envConfTest() {
    environmentVariables.set("RSS_HOME", (new File(confFile)).getParent());
    shuffleServerConf.loadConfFromFile(null);
    assertEquals(1234, shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    assertEquals("HDFS", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_TYPE));
    assertEquals("/var/tmp/test", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH));

    environmentVariables.set("RSS_HOME", (new File(confFile)).getParent() + "/wrong_dir/");
    assertFalse(shuffleServerConf.loadConfFromFile(null));
  }

  @Test
  public void confTest() {
    assertTrue(shuffleServerConf.loadConfFromFile(confFile));
    assertEquals(1234, shuffleServerConf.getInteger(ShuffleServerConf.RPC_SERVER_PORT));
    assertEquals("FILE", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_TYPE));
    assertEquals("/var/tmp/test", shuffleServerConf.getString(ShuffleServerConf.RSS_STORAGE_BASE_PATH));
    assertFalse(shuffleServerConf.loadConfFromFile("/var/tmp/null"));
    assertEquals(2, shuffleServerConf.getLong(ShuffleServerConf.SERVER_BUFFER_CAPACITY));

    thrown.expect(NullPointerException.class);
    shuffleServerConf.getInteger(ShuffleServerConf.SERVER_PARTITION_BUFFER_SIZE);

  }
}
