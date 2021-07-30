package com.tencent.rss.storage.handler.api;

import com.tencent.rss.common.ShuffleDataResult;

public interface ServerReadHandler {

  ShuffleDataResult getShuffleData(int segmentIndex);
}
