package com.tencent.rss.coordinator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleClusterManager implements ClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleClusterManager.class);

  private final Map<String, ServerNode> servers = Maps.newConcurrentMap();
  private long heartbeatTimeout;
  private ScheduledExecutorService scheduledExecutorService;

  public SimpleClusterManager(long heartbeatTimeout) {
    this.heartbeatTimeout = heartbeatTimeout;
    // the thread for checking if shuffle server report heartbeat in time
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(
        () -> nodesCheck(), heartbeatTimeout / 3,
        heartbeatTimeout / 3, TimeUnit.MILLISECONDS);
  }

  private void nodesCheck() {
    try {
      long timestamp = System.currentTimeMillis();
      Set<String> deleteIds = Sets.newHashSet();
      for (ServerNode sn : servers.values()) {
        if (timestamp - sn.getTimestamp() > heartbeatTimeout) {
          LOG.warn("Heartbeat timeout detect, " + sn + " will be removed from node list.");
          deleteIds.add(sn.getId());
        }
      }
      for (String serverId : deleteIds) {
        servers.remove(serverId);
      }

      CoordinatorMetrics.gaugeTotalServerNum.set(servers.size());
    } catch (Exception e) {
      LOG.warn("Error happened in nodesCheck", e);
    }
  }

  @Override
  public void add(ServerNode node) {
    servers.put(node.getId(), node);
  }

  @Override
  public List<ServerNode> getServerList(int hint) {
    List<ServerNode> orderServers = Lists.newArrayList(servers.values());
    Collections.sort(orderServers);
    return orderServers.subList(0, Math.min(hint, orderServers.size()));
  }

  @Override
  public int getNodesNum() {
    return servers.size();
  }

  @Override
  public List<ServerNode> list() {
    return Lists.newArrayList(servers.values());
  }

  @VisibleForTesting
  void clear() {
    servers.clear();
  }

  @Override
  public void shutdown() {
    scheduledExecutorService.shutdown();
  }
}
