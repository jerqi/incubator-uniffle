package com.tencent.rss.coordinator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleClusterManager implements ClusterManager {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleClusterManager.class);

  private final Map<String, ServerNode> servers = Maps.newConcurrentMap();
  private Set<String> excludeNodes = Sets.newConcurrentHashSet();
  private AtomicLong excludeLastModify = new AtomicLong(0L);
  private long heartbeatTimeout;
  private ScheduledExecutorService scheduledExecutorService;
  private ScheduledExecutorService checkNodesExecutorService;

  public SimpleClusterManager(CoordinatorConf conf) {
    this.heartbeatTimeout = conf.getLong(CoordinatorConf.COORDINATOR_HEARTBEAT_TIMEOUT);
    // the thread for checking if shuffle server report heartbeat in time
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SimpleClusterManager-%d").build());
    scheduledExecutorService.scheduleAtFixedRate(
        () -> nodesCheck(), heartbeatTimeout / 3,
        heartbeatTimeout / 3, TimeUnit.MILLISECONDS);

    String excludeNodesPath = conf.getString(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_FILE_PATH, "");
    if (!StringUtils.isEmpty(excludeNodesPath)) {
      long updateNodesInterval = conf.getLong(CoordinatorConf.COORDINATOR_EXCLUDE_NODES_CHECK_INTERVAL);
      checkNodesExecutorService = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("UpdateExcludeNodes-%d").build());
      checkNodesExecutorService.scheduleAtFixedRate(
          () -> updateExcludeNodes(excludeNodesPath), updateNodesInterval, updateNodesInterval, TimeUnit.MILLISECONDS);
    }
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

  private void updateExcludeNodes(String path) {
    try {
      File excludeNodesFile = new File(path);
      if (excludeNodesFile.exists()) {
        // don't parse same file twice
        if (excludeLastModify.get() != excludeNodesFile.lastModified()) {
          parseExcludeNodesFile(excludeNodesFile);
        }
      } else {
        excludeNodes = Sets.newConcurrentHashSet();
      }
    } catch (Exception e) {
      LOG.warn("Error when update exclude nodes", e);
    }
  }

  private void parseExcludeNodesFile(File excludeNodesFile) {
    try {
      Set<String> nodes = Sets.newConcurrentHashSet();
      try (BufferedReader br = new BufferedReader(new FileReader(excludeNodesFile))) {
        String line;
        while ((line = br.readLine()) != null) {
          if (!StringUtils.isEmpty(line)) {
            nodes.add(line);
          }
        }
      }
      // update exclude nodes and last modify time
      excludeNodes = nodes;
      excludeLastModify.set(excludeNodesFile.lastModified());
      LOG.info("Update exclude nodes and " + excludeNodes.size() + " nodes was found");
    } catch (Exception e) {
      LOG.warn("Error when parse file " + excludeNodesFile.getAbsolutePath(), e);
    }
  }

  @Override
  public void add(ServerNode node) {
    servers.put(node.getId(), node);
  }

  @Override
  public List<ServerNode> getServerList(int hint) {
    List<ServerNode> availableNodes = Lists.newArrayList();
    for (ServerNode node : servers.values()) {
      if (!excludeNodes.contains(node.getId())) {
        availableNodes.add(node);
      }
    }
    List<ServerNode> orderServers = Lists.newArrayList(availableNodes);
    Collections.sort(orderServers);
    return orderServers.subList(0, Math.min(hint, orderServers.size()));
  }

  public Set<String> getExcludeNodes() {
    return excludeNodes;
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
