package com.tencent.rss.coordinator;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationManager.class);
  private long expired;
  private Map<String, Long> appIds = Maps.newConcurrentMap();
  private ScheduledExecutorService scheduledExecutorService;

  public ApplicationManager(CoordinatorConf conf) {
    expired = conf.getLong(CoordinatorConf.COORDINATOR_APP_EXPIRED);
    // the thread for checking application status
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(
        () -> statusCheck(), expired / 2, expired / 2, TimeUnit.MILLISECONDS);
  }

  public void refreshAppId(String appId) {
    if (!appIds.containsKey(appId)) {
      CoordinatorMetrics.counterTotalAppNum.inc();
    }
    appIds.put(appId, System.currentTimeMillis());
  }

  public Set<String> getAppIds() {
    return appIds.keySet();
  }

  private void statusCheck() {
    LOG.info("Start to check application status for " + appIds);
    long current = System.currentTimeMillis();
    Set<String> expiredAppIds = Sets.newHashSet();
    for (Map.Entry<String, Long> entry : appIds.entrySet()) {
      long lastReport = entry.getValue();
      if (current - lastReport > expired) {
        expiredAppIds.add(entry.getKey());
      }
    }
    for (String appId : expiredAppIds) {
      LOG.info("Remove expired application:" + appId);
      appIds.remove(appId);
    }
    CoordinatorMetrics.gaugeRunningAppNum.set(appIds.size());
  }
}
