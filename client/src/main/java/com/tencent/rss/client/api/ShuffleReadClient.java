package com.tencent.rss.client.api;

public interface ShuffleReadClient {

  byte[] readShuffleBlockData();

  void checkProcessedBlockIds();

  void close();

  void logStatics();
}
