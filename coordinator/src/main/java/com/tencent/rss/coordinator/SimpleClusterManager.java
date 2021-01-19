package com.tencent.rss.coordinator;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SimpleClusterManager implements ClusterManager {

  private final Map<String, ServerNode> serverMap;
  private final long aliveThreshold;
  private final int usableThreshold;

  public SimpleClusterManager(long aliveThreshold, int availableThreshold) {
    serverMap = new ConcurrentHashMap<>();
    this.aliveThreshold = aliveThreshold;
    this.usableThreshold = availableThreshold;
  }

  public void add(ServerNode node) {
    String id = node.getId();
    serverMap.put(id, node);
  }

  public void update(ServerNode node) {
    add(node);
  }

  public void remove(ServerNode node) {
    String id = node.getId();
    serverMap.remove(id);
  }

  public List<ServerNode> get(int hint) {
    List<ServerNode> servers = new ArrayList<>(serverMap.values());
    long currentTimestamp = System.currentTimeMillis();
    List<ServerNode> availableServers =
        servers.stream().filter(s -> isAvailable(s, currentTimestamp)).collect(Collectors.toList());
    availableServers.sort(Comparator.comparingInt(ServerNode::getScore).reversed());
    return availableServers.subList(0, Math.min(hint, availableServers.size()));
  }

  public int getNodesNum() {
    return serverMap.size();
  }

  public List<ServerNode> list() {
    return new LinkedList<>(serverMap.values());
  }

  private boolean isAlive(ServerNode node, long curTimestamp) {
    return curTimestamp - node.getTimestamp() < aliveThreshold;
  }

  private boolean isUsable(ServerNode node) {
    return node.getScore() >= usableThreshold;
  }

  private boolean isAvailable(ServerNode node, long curTimestamp) {
    return isAlive(node, curTimestamp) && isUsable(node);
  }

  @VisibleForTesting
  void addNodes(List<ServerNode> nodes) {
    for (ServerNode node : nodes) {
      add(node);
    }
  }

  @VisibleForTesting
  void removeNodes(List<ServerNode> nodes) {
    for (ServerNode node : nodes) {
      remove(node);
    }
  }

  @VisibleForTesting
  void clear() {
    serverMap.clear();
  }

}
