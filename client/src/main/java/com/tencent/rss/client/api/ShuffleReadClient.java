package com.tencent.rss.client.api;

public interface ShuffleReadClient {

  void checkExpectedBlockIds();

  byte[] readShuffleData();

  void checkProcessedBlockIds();

  void close();

  void logStatics();
}
