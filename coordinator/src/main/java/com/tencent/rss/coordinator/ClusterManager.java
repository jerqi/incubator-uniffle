package com.tencent.rss.coordinator;

import java.util.List;

public interface ClusterManager {

  /**
   * Add a server to the cluster.
   *
   * @param shuffleServerInfo server info
   */
  void add(ServerNode shuffleServerInfo);

  /**
   * Update server status.
   *
   * @param shuffleServerInfo server info
   */
  void update(ServerNode shuffleServerInfo);

  /**
   * Remove a server from the cluster.
   *
   * @param shuffleServerInfo server info
   */
  void remove(ServerNode shuffleServerInfo);

  /**
   * Get list of servers from the cluster, the list size if smaller or equal than hint.
   *
   * @param hint maximum number of servers to be gotten from the cluster
   * @return list of available server nodes
   */
  List<ServerNode> get(int hint);

  /**
   * @return number of server nodes in the cluster
   */
  int getNodesNum();

  /**
   * @return list all server nodes in the cluster
   */
  List<ServerNode> list();

}