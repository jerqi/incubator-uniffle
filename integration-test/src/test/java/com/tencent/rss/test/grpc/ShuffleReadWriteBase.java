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
import java.util.concurrent.atomic.AtomicLong;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public abstract class ShuffleReadWriteBase extends IntegrationTestBase {

  private static AtomicLong ATOMIC_LONG = new AtomicLong(0L);
  protected List<ShuffleServerInfo> mockSSI =
      Lists.newArrayList(new ShuffleServerInfo("id", "host", 0));

  protected List<ShuffleBlockInfo> createShuffleBlockList(int shuffleId, int partitionId, long taskAttemptId,
      int blockNum, int length, Roaring64NavigableMap blockIdBitmap, Map<Long, byte[]> dataMap,
      List<ShuffleServerInfo> shuffleServerInfoList) {
    List<ShuffleBlockInfo> shuffleBlockInfoList = Lists.newArrayList();
    for (int i = 0; i < blockNum; i++) {
      byte[] buf = new byte[length];
      new Random().nextBytes(buf);
      long blockId = ATOMIC_LONG.getAndIncrement();
      blockIdBitmap.addLong(blockId);
      dataMap.put(blockId, buf);
      shuffleBlockInfoList.add(new ShuffleBlockInfo(
          shuffleId, partitionId, blockId, length, ChecksumUtils.getCrc32(buf),
          buf, shuffleServerInfoList, length, 10, taskAttemptId));
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
