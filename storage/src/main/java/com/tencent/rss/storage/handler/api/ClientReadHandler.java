package com.tencent.rss.storage.handler.api;

import com.tencent.rss.common.ShuffleDataResult;
import java.util.Set;

public interface ClientReadHandler {

  ShuffleDataResult readShuffleData(Set<Long> expectedBlockIds);

  void close();
}
