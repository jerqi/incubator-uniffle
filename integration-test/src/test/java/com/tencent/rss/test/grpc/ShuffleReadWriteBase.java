package com.tencent.rss.test.grpc;

import com.google.common.collect.Lists;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.ChecksumUtils;
import com.tencent.rss.test.IntegrationTestBase;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ShuffleReadWriteBase extends IntegrationTestBase {

  private static AtomicLong ATOMIC_LONG = new AtomicLong(0L);

  protected List<ShuffleBlockInfo> createShuffleBlockList(int shuffleId, int partitionId,
      int blockNum, int length, Set<Long> blockIds, Map<Long, byte[]> dataMap) {
    List<ShuffleServerInfo> shuffleServerInfoList =
        Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = ATOMIC_LONG.getAndIncrement();
      blockIds.add(blockId);
      dataMap.put(blockId, buf);
      shuffleBlockInfoList.add(new ShuffleBlockInfo(
          shuffleId, partitionId, blockId, length, ChecksumUtils.getCrc32(buf), buf, shuffleServerInfoList, length));
    }
    return shuffleBlockInfoList;
  }

  protected boolean compareByte(byte[] expected, ByteBuffer buffer) {
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != buffer.get(i)) {
        return false;
      }
    }
    return true;
  }

}
