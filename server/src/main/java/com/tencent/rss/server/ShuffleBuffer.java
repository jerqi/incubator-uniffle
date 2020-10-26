package com.tencent.rss.server;

import com.tencent.rss.proto.RssProtos.ShuffleBlock;

import java.util.List;
import java.util.Map;

public class ShuffleBuffer {
  private int capacity;
  private int ttl;
  private Map<String, List<ShuffleBlock>> partitionBuffers;

  public ShuffleBuffer(int capacity, int ttl) {
    this.capacity = capacity;
    this.ttl = ttl;
  }
}
