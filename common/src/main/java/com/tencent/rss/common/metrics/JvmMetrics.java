package com.tencent.rss.common.metrics;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.BufferPoolsExports;
import io.prometheus.client.hotspot.ClassLoadingExports;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryAllocationExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import io.prometheus.client.hotspot.StandardExports;
import io.prometheus.client.hotspot.ThreadExports;
import io.prometheus.client.hotspot.VersionInfoExports;

public class JvmMetrics {
  private static CollectorRegistry collectorRegistry;
  private static boolean initialized = false;

  public JvmMetrics() {
  }

  public static CollectorRegistry getCollectorRegistry() {
    return collectorRegistry;
  }

  public static void register() {
    register(CollectorRegistry.defaultRegistry, false);

  }

  public static void register(CollectorRegistry collectorRegistry) {
    register(collectorRegistry, false);

  }

  public static synchronized void register(CollectorRegistry collectorRegistry, boolean verbose) {
    if (!initialized) {
      JvmMetrics.collectorRegistry = collectorRegistry;
      if (verbose) {
        registerVerbose(collectorRegistry);
      } else {
        registerDefault(collectorRegistry);
      }
      initialized = true;
    }

  }

  private static void registerDefault(CollectorRegistry registry) {
    (new StandardExports()).register(registry);
    (new BufferPoolsExports()).register(registry);
    (new GarbageCollectorExports()).register(registry);
    (new ThreadExports()).register(registry);
    (new VersionInfoExports()).register(registry);
  }

  private static void registerVerbose(CollectorRegistry registry) {
    (new StandardExports()).register(registry);
    (new MemoryPoolsExports()).register(registry);
    (new MemoryAllocationExports()).register(registry);
    (new BufferPoolsExports()).register(registry);
    (new GarbageCollectorExports()).register(registry);
    (new ThreadExports()).register(registry);
    (new ClassLoadingExports()).register(registry);
    (new VersionInfoExports()).register(registry);
  }

}
