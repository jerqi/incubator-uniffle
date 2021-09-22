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
   * Get list of servers from the cluster, the list size if smaller or equal than hint.
   *
   * @param expectedNum maximum number of servers to be gotten from the cluster
   * @return list of available server nodes
   */
  List<ServerNode> getServerList(int expectedNum, Set<String> requiredTags);

  /**
   * @return number of server nodes in the cluster
   */
  int getNodesNum();

  /**
   * @return list all server nodes in the cluster
   */
  List<ServerNode> list();

  void shutdown();
}
