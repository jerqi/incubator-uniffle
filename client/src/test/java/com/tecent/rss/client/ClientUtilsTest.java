package com.tecent.rss.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tecent.rss.client.util.ClientUtils;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class ClientUtilsTest {

  @Test
  public void getBlockIdTest() {
    assertTrue(
        9223372034707292159L == ClientUtils.getBlockId(Integer.MAX_VALUE, Integer.MAX_VALUE));
    assertTrue(0L == ClientUtils.getBlockId(0, 0));
    assertTrue(2147483647L == ClientUtils.getBlockId(0, Integer.MAX_VALUE));
    assertTrue(6442450943L == ClientUtils.getBlockId(1, Integer.MAX_VALUE));
    assertTrue(9223372032559808513L == ClientUtils.getBlockId(Integer.MAX_VALUE, 1));
    assertTrue(5299989644498L == ClientUtils.getBlockId(1234, 1234));
  }

  @Test
  public void getAtomicIntegerTest() {
    int atomicId = ClientUtils.getAtomicInteger();
    assertTrue((atomicId + 1) == ClientUtils.getAtomicInteger());
  }


  @Test
  public void transformTest() throws Exception {
    Map<Integer, Set<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(1, Sets.newHashSet(1L, 2L, 3L));
    partitionToBlockIds.put(2, Sets.newHashSet(4L, 5L, 6L));
    partitionToBlockIds.put(3, Sets.newHashSet(7L, 8L, 9L));
    String jsonStr = ClientUtils.transBlockIdsToJson(partitionToBlockIds);
    Map<Integer, Set<Long>> actualMap = ClientUtils.getBlockIdsFromJson(jsonStr);
    assertEquals(partitionToBlockIds, actualMap);
  }

}
