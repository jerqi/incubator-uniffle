package com.tencent.rss.coordinator;

public class ClusterManagerFactory {

  CoordinatorConf conf;

  public ClusterManagerFactory(CoordinatorConf conf) {
    this.conf = conf;
  }

  public ClusterManager getClusterManager() {
    long aliveThreshold = conf.getLong(CoordinatorConf.ALIVE_THRESHOLD);
    int usableThreshold = conf.getInteger(CoordinatorConf.USABLE_THRESHOLD);
    return new SimpleClusterManager(aliveThreshold, usableThreshold);
  }
}
