package com.tencent.rss.test;

import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.coordinator.CoordinatorServer;
import com.tencent.rss.server.ShuffleServer;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.HdfsTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class IntegrationTestBase extends HdfsTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class);
  protected static int COORDINATOR_PORT = 19999;
  protected static String LOCALHOST = "127.0.0.1";
  private static ShuffleServer shuffleServer;
  private static CoordinatorServer coordinator;

  @BeforeClass
  public static void setupServers() throws Exception {
    // Load configuration from config files
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setString("rss.coordinator.port", Integer.toString(COORDINATOR_PORT));
    coordinatorConf.setString("rss.shuffle.data.replica", "1");
    // Start the coordinator service
    coordinator = new CoordinatorServer(coordinatorConf);
    coordinator.start();

    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setString("rss.server.port", "20001");
    serverConf.setString("rss.storage.type", "FILE");
    serverConf.setString("rss.data.storage.basePath", HDFS_URI + "rss/test");
    serverConf.setString("rss.buffer.capacity", "671088640");
    serverConf.setString("rss.buffer.size", "67108864");
    serverConf.setString("rss.coordinator.ip", "127.0.0.1");
    serverConf.setString("rss.coordinator.port", Integer.toString(COORDINATOR_PORT));
    serverConf.setString("rss.heartbeat.delay", "1000");
    serverConf.setString("rss.heartbeat.interval", "2000");
    serverConf.setString("jetty.http.port", "18080");
    serverConf.setString("jetty.corePool.size", "64");

    shuffleServer = new ShuffleServer(serverConf);
    shuffleServer.start();
  }

  @AfterClass
  public static void shutdownServers() throws Exception {
    shuffleServer.stopServer();
    coordinator.stop();
  }
}