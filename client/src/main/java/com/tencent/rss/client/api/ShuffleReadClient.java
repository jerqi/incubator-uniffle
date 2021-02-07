package com.tencent.rss.client.api;

public interface ShuffleReadClient {

  void checkExpectedBlockIds();

  byte[] readShuffleBlockData();

  void checkProcessedBlockIds();

  void close();

  void logStatics();
}
