package com.tecent.rss.client;

public interface ShuffleReadClient {

  void checkExpectedBlockIds();

  byte[] readShuffleData();

  void checkProcessedBlockIds();

  void close();
}
