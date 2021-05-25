package com.tencent.rss.test;

import com.google.common.collect.Lists;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.coordinator.CoordinatorServer;
import com.tencent.rss.server.ShuffleServer;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.HdfsTestBase;
import com.tencent.rss.storage.util.StorageType;
import java.util.List;
import org.junit.AfterClass;

abstract public class IntegrationTestBase extends HdfsTestBase {

  protected static final int JETTY_PORT = 19998;
  protected static int COORDINATOR_PORT = 19999;
  protected static int SHUFFLE_SERVER_PORT = 20001;
  protected static String LOCALHOST = "127.0.0.1";
  protected static List<ShuffleServer> shuffleServers = Lists.newArrayList();
  protected static List<CoordinatorServer> coordinators = Lists.newArrayList();

  public static void startServers() throws Exception {
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.start();
    }
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.start();
    }
  }

  @AfterClass
  public static void shutdownServers() throws Exception {
    for (CoordinatorServer coordinator : coordinators) {
      coordinator.stopServer();
    }
    for (ShuffleServer shuffleServer : shuffleServers) {
      shuffleServer.stopServer();
    }
    shuffleServers = Lists.newArrayList();
    coordinators = Lists.newArrayList();
  }

  protected static CoordinatorConf getCoordinatorConf() {
    CoordinatorConf coordinatorConf = new CoordinatorConf();
    coordinatorConf.setInteger("rss.rpc.server.port", COORDINATOR_PORT);
    coordinatorConf.setInteger("rss.jetty.http.port", JETTY_PORT);
    return coordinatorConf;
  }

  protected static ShuffleServerConf getShuffleServerConf() {
    ShuffleServerConf serverConf = new ShuffleServerConf();
    serverConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT);
    serverConf.setString("rss.storage.type", StorageType.HDFS.name());
    serverConf.setString("rss.storage.basePath", HDFS_URI + "rss/test");
    serverConf.setString("rss.server.buffer.capacity", "671088640");
    serverConf.setString("rss.server.buffer.spill.threshold", "335544320");
    serverConf.setString("rss.server.read.buffer.capacity", "335544320");
    serverConf.setString("rss.server.partition.buffer.size", "67108864");
    serverConf.setString("rss.coordinator.ip", "127.0.0.1");
    serverConf.setInteger("rss.coordinator.port", COORDINATOR_PORT);
    serverConf.setString("rss.server.heartbeat.delay", "1000");
    serverConf.setString("rss.server.heartbeat.interval", "1000");
    serverConf.setInteger("rss.jetty.http.port", 18080);
    serverConf.setInteger("rss.jetty.corePool.size", 64);
    serverConf.setInteger("rss.rpc.executor.size", 10);
    serverConf.setString("rss.server.hadoop.dfs.replication", "2");
    return serverConf;
  }

  protected static void createCoordinatorServer(CoordinatorConf coordinatorConf) throws Exception {
    coordinators.add(new CoordinatorServer(coordinatorConf));
  }

  protected static void createShuffleServer(ShuffleServerConf serverConf) throws Exception {
    shuffleServers.add(new ShuffleServer(serverConf));
  }
}