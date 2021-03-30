package com.tencent.rss.coordinator;

public class ClusterManagerFactory {

  CoordinatorConf conf;

  public ClusterManagerFactory(CoordinatorConf conf) {
    this.conf = conf;
  }

  public ClusterManager getClusterManager() {
    long heartbeatTimeout = conf.getLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT);
    return new SimpleClusterManager(heartbeatTimeout);
  }
}
