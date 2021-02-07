package com.tencent.rss.storage.handler.api;

import com.tencent.rss.common.ShufflePartitionedBlock;
import java.io.IOException;
import java.util.List;

public interface ShuffleWriteHandler {

  /**
   * Write the blocks to storage
   *
   * @param shuffleBlocks blocks to storage
   * @throws IOException
   * @throws IllegalStateException
   */
  void write(List<ShufflePartitionedBlock> shuffleBlocks) throws IOException, IllegalStateException;

  long getAccessTime();
}
