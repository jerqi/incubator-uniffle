package com.tencent.rss.coordinator;

public class AssignmentStrategyFactory {

  private CoordinatorConf conf;
  private ClusterManager clusterManager;

  public AssignmentStrategyFactory(CoordinatorConf conf, ClusterManager clusterManager) {
    this.conf = conf;
    this.clusterManager = clusterManager;
  }

  public AssignmentStrategy getAssignmentStrategy() {
    String strategy = conf.getString(CoordinatorConf.ASSIGNMENT_STRATEGY);
    if (strategy.equals(StrategyName.BASIC.name())) {
      return new BasicAssignmentStrategy(clusterManager);
    } else {
      throw new UnsupportedOperationException("Unsupported assignment strategy.");
    }
  }

  private enum StrategyName {
    BASIC
  }

}
