package com.tencent.rss.client.api;

import com.tencent.rss.client.response.CompressedShuffleBlock;

public interface ShuffleReadClient {

  CompressedShuffleBlock readShuffleBlockData();

  void checkProcessedBlockIds();

  void close();

  void logStatics();
}
