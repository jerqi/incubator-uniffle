package com.tencent.rss.coordinator;

public class ClusterManagerFactory {

  CoordinatorConf conf;

  public ClusterManagerFactory(CoordinatorConf conf) {
    this.conf = conf;
  }

  public ClusterManager getClusterManager() {
    return new SimpleClusterManager(conf);
  }
}
