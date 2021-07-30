package com.tencent.rss.storage.handler.api;

import com.tencent.rss.common.ShuffleDataResult;

public interface ClientReadHandler {

  ShuffleDataResult readShuffleData(int segmentIndex);

  void close();
}
