package com.tencent.rss.coordinator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.tencent.rss.proto.RssProtos;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.Test;

public class PartitionRangeAssignmentTest {

  @Test
  public void test() {
    SortedMap sortedMap = new TreeMap();
    for (int i = 0; i < 9; i = i + 3) {
      PartitionRange range = new PartitionRange(i, i + 2);
      List<ServerNode> nodes = Collections.singletonList(new ServerNode(
          String.valueOf(i), "", i / 3, 0, 0, 0, 0));
      sortedMap.put(range, nodes);
    }

    PartitionRangeAssignment partitionRangeAssignment = new PartitionRangeAssignment(sortedMap);
    List<RssProtos.PartitionRangeAssignment> res = partitionRangeAssignment.convertToGrpcProto();
    assertEquals(3, res.size());

    for (int i = 0; i < 3; ++i) {
      RssProtos.PartitionRangeAssignment pra = res.get(i);
      assertEquals(1, pra.getServerCount());
      assertEquals(i, pra.getServer(0).getPort());
      assertEquals(3 * i, pra.getStartPartition());
      assertEquals(3 * i + 2, pra.getEndPartition());
    }

    partitionRangeAssignment = new PartitionRangeAssignment(null);
    res = partitionRangeAssignment.convertToGrpcProto();
    assertTrue(res.isEmpty());
  }
}
