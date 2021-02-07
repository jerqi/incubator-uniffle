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

  protected static final int JETTY_PORT = 19998;
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBase.class);
  protected static int COORDINATOR_PORT = 19999;
  protected static int SHUFFLE_SERVER_PORT = 20001;
  protected static String LOCALHOST = "127.0.0.1";
  private static ShuffleServer shuffleServer;
  private static CoordinatorServer coordinator;

  @BeforeClass
  public static void setupServers() throws Exception {
    // Load configuration from config files
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setInteger("rss.rpc.server.port", COORDINATOR_PORT);
    coordinatorConf.setInteger("rss.jetty.http.port", JETTY_PORT);
    coordinatorConf.setInteger("rss.coordinator.server.replica", 1);
    // Start the coordinator service
    coordinator = new CoordinatorServer(coordinatorConf);
    coordinator.start();

    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT);
    serverConf.setString("rss.storage.type", "HDFS");
    serverConf.setString("rss.storage.basePath", HDFS_URI + "rss/test");
    serverConf.setString("rss.server.buffer.capacity", "671088640");
    serverConf.setString("rss.server.buffer.size", "67108864");
    serverConf.setString("rss.server.coordinator.ip", "127.0.0.1");
    serverConf.setInteger("rss.server.coordinator.port", COORDINATOR_PORT);
    serverConf.setString("rss.server.heartbeat.delay", "1000");
    serverConf.setString("rss.server.heartbeat.interval", "2000");
    serverConf.setInteger("rss.jetty.http.port", 18080);
    serverConf.setInteger("rss.jetty.corePool.size", 64);

    shuffleServer = new ShuffleServer(serverConf);
    shuffleServer.start();
  }

  @AfterClass
  public static void shutdownServers() throws Exception {
    shuffleServer.stopServer();
    coordinator.stopServer();
  }
}