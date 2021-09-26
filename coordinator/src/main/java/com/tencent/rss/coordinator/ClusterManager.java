package com.tencent.rss.coordinator;

import java.util.List;
import java.util.Set;

public interface ClusterManager {

  /**
   * Add a server to the cluster.
   *
   * @param shuffleServerInfo server info
   */
  void add(ServerNode shuffleServerInfo);

  /**
   * Get available nodes from the cluster
   *
   * @param requiredTags tags for filter
   * @return list of available server nodes
   */
  List<ServerNode> getServerList(Set<String> requiredTags);

  /**
   * @return number of server nodes in the cluster
   */
  int getNodesNum();

  /**
   * @return list all server nodes in the cluster
   */
  List<ServerNode> list();

  int getShuffleNodesMax();

  void shutdown();
}
