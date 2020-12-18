package com.tencent.rss.server;

public class MetricsTestBase {
  static {
    ShuffleServerMetrics.register();
  }

}
