package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;

import java.util.Objects;
import org.junit.Test;

public class CoordinatorConfTest {

  @Test
  public void test() {
    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("coordinator.conf")).getFile();
    CoordinatorConf conf = new CoordinatorConf(filePath);

    // test base conf
    assertEquals(9527, conf.getInteger(CoordinatorConf.RPC_SERVER_PORT));
    assertEquals("testRpc", conf.getString(CoordinatorConf.RPC_SERVER_TYPE));
    assertEquals(9526, conf.getInteger(CoordinatorConf.JETTY_HTTP_PORT));

    // test coordinator specific conf
    assertEquals("/a/b/c", conf.getString(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_FILE_PATH));
    assertEquals(37, conf.getInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX));
    assertEquals(123, conf.getLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT));

    // test default conf
    assertEquals("BASIC", conf.getString(CoordinatorConf.COORDINATOR_ASSIGNMENT_STRATEGY));
    assertEquals(256, conf.getInteger(CoordinatorConf.JETTY_CORE_POOL_SIZE));
    assertEquals(60 * 1000, conf.getLong(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL));

  }

}
