package com.tencent.rss.storage.handler.api;

import com.tencent.rss.storage.common.BufferSegment;
import java.util.Map;
import java.util.Set;

public interface ShuffleReadHandler {

  Map<Long, BufferSegment> getBlockIdToBufferSegment();

  Set<Long> getAllBlockIds();

  byte[] readShuffleData(Set<Long> expectedBlockIds);

  void close();
}
