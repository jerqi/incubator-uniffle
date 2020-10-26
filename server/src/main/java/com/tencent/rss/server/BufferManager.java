package com.tencent.rss.server;

public class BufferManager {
  private int capacity;
  private int bufferSize;
  private int bufferTTL;
  private int cnt;

  private BufferManager() {}

  private static class LazyHolder {
    static final BufferManager INSTANCE = new BufferManager();
  }

  public static BufferManager instance() {
    return LazyHolder.INSTANCE;
  }

  public boolean init() {
    // load config and init capacity and buffSize
    return true;
  }

}
