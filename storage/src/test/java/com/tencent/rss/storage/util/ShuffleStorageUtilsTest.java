package com.tencent.rss.storage.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import com.tencent.rss.storage.handler.impl.FileReadSegment;
import java.util.List;
import java.util.Set;
import org.junit.Test;

public class ShuffleStorageUtilsTest {

  @Test
  public void mergeSegmentsTest() {
    List<FileBasedShuffleSegment> segments = Lists.newArrayList(
        new FileBasedShuffleSegment(1, 0, 40, 0));
    List<FileReadSegment> fileSegments = ShuffleStorageUtils.mergeSegments("path", segments, 100);
    assertEquals(1, fileSegments.size());
    for (FileReadSegment seg : fileSegments) {
      assertEquals(0, seg.getOffset());
      assertEquals(40, seg.getLength());
      assertEquals("path", seg.getPath());
      List<BufferSegment> bufferSegments = seg.getBufferSegments();
      assertEquals(1, bufferSegments.size());
      assertEquals(new BufferSegment(1, 0, 40, 0), bufferSegments.get(0));
    }

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(1, 0, 40, 0),
        new FileBasedShuffleSegment(2, 40, 40, 0),
        new FileBasedShuffleSegment(3, 80, 20, 0));
    fileSegments = ShuffleStorageUtils.mergeSegments("path", segments, 100);
    assertEquals(1, fileSegments.size());
    for (FileReadSegment seg : fileSegments) {
      assertEquals(0, seg.getOffset());
      assertEquals(100, seg.getLength());
      assertEquals("path", seg.getPath());
      List<BufferSegment> bufferSegments = seg.getBufferSegments();
      assertEquals(3, bufferSegments.size());
      Set<Long> testedBlockIds = Sets.newHashSet();
      for (BufferSegment segment : bufferSegments) {
        if (segment.getBlockId() == 1) {
          assertTrue(segment.equals(new BufferSegment(1, 0, 40, 0)));
          testedBlockIds.add(1L);
        } else if (segment.getBlockId() == 2) {
          assertTrue(segment.equals(new BufferSegment(2, 40, 40, 0)));
          testedBlockIds.add(2L);
        } else if (segment.getBlockId() == 3) {
          assertTrue(segment.equals(new BufferSegment(3, 80, 20, 0)));
          testedBlockIds.add(3L);
        }
      }
      assertEquals(3, testedBlockIds.size());
    }

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(1, 0, 40, 0),
        new FileBasedShuffleSegment(2, 40, 40, 0),
        new FileBasedShuffleSegment(3, 80, 20, 0),
        new FileBasedShuffleSegment(4, 100, 20, 0));
    fileSegments = ShuffleStorageUtils.mergeSegments("path", segments, 100);
    assertEquals(2, fileSegments.size());
    boolean tested = false;
    for (FileReadSegment seg : fileSegments) {
      if (seg.getOffset() == 100) {
        tested = true;
        assertEquals(20, seg.getLength());
        assertEquals("path", seg.getPath());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(1, bufferSegments.size());
        assertTrue(bufferSegments.get(0).equals(new BufferSegment(4, 0, 20, 0)));
      }
    }
    assertTrue(tested);

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(1, 0, 40, 0),
        new FileBasedShuffleSegment(2, 40, 40, 0),
        new FileBasedShuffleSegment(3, 80, 20, 0),
        new FileBasedShuffleSegment(4, 100, 20, 0),
        new FileBasedShuffleSegment(5, 120, 100, 0));
    fileSegments = ShuffleStorageUtils.mergeSegments("path", segments, 100);
    assertEquals(2, fileSegments.size());
    tested = false;
    for (FileReadSegment seg : fileSegments) {
      if (seg.getOffset() == 100) {
        tested = true;
        assertEquals(120, seg.getLength());
        assertEquals("path", seg.getPath());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(2, bufferSegments.size());
        Set<Long> testedBlockIds = Sets.newHashSet();
        for (BufferSegment segment : bufferSegments) {
          if (segment.getBlockId() == 4) {
            assertTrue(segment.equals(new BufferSegment(4, 0, 20, 0)));
            testedBlockIds.add(4L);
          } else if (segment.getBlockId() == 5) {
            assertTrue(segment.equals(new BufferSegment(5, 20, 100, 0)));
            testedBlockIds.add(5L);
          }
        }
        assertEquals(2, testedBlockIds.size());
      }
    }
    assertTrue(tested);

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(1, 10, 40, 0),
        new FileBasedShuffleSegment(2, 80, 20, 0),
        new FileBasedShuffleSegment(3, 500, 120, 0),
        new FileBasedShuffleSegment(4, 700, 20, 0));
    fileSegments = ShuffleStorageUtils.mergeSegments("path", segments, 100);
    assertEquals(3, fileSegments.size());
    Set<Long> expectedOffset = Sets.newHashSet(10L, 500L, 700L);
    for (FileReadSegment seg : fileSegments) {
      if (seg.getOffset() == 10) {
        assertEquals(90, seg.getLength());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(2, bufferSegments.size());
        Set<Long> testedBlockIds = Sets.newHashSet();
        for (BufferSegment segment : bufferSegments) {
          if (segment.getBlockId() == 1) {
            assertTrue(segment.equals(new BufferSegment(1, 0, 40, 0)));
            testedBlockIds.add(1L);
          } else if (segment.getBlockId() == 2) {
            assertTrue(segment.equals(new BufferSegment(2, 70, 20, 0)));
            testedBlockIds.add(2L);
          }
        }
        assertEquals(2, testedBlockIds.size());
        expectedOffset.remove(10L);
      }
      if (seg.getOffset() == 500) {
        assertEquals(120, seg.getLength());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(1, bufferSegments.size());
        assertTrue(bufferSegments.get(0).equals(new BufferSegment(3, 0, 120, 0)));
        expectedOffset.remove(500L);
      }
      if (seg.getOffset() == 700) {
        assertEquals(20, seg.getLength());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(1, bufferSegments.size());
        assertTrue(bufferSegments.get(0).equals(new BufferSegment(4, 0, 20, 0)));
        expectedOffset.remove(700L);
      }
    }
    assertTrue(expectedOffset.isEmpty());

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(5, 500, 120, 0),
        new FileBasedShuffleSegment(3, 630, 10, 0),
        new FileBasedShuffleSegment(2, 80, 20, 0),
        new FileBasedShuffleSegment(1, 10, 40, 0),
        new FileBasedShuffleSegment(6, 769, 20, 0),
        new FileBasedShuffleSegment(4, 700, 20, 0));
    fileSegments = ShuffleStorageUtils.mergeSegments("path", segments, 100);
    assertEquals(4, fileSegments.size());
    expectedOffset = Sets.newHashSet(10L, 500L, 630L, 700L);
    for (FileReadSegment seg : fileSegments) {
      if (seg.getOffset() == 10) {
        assertEquals(90, seg.getLength());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(2, bufferSegments.size());
        Set<Long> testedBlockIds = Sets.newHashSet();
        for (BufferSegment segment : bufferSegments) {
          if (segment.getBlockId() == 1) {
            assertTrue(segment.equals(new BufferSegment(1, 0, 40, 0)));
            testedBlockIds.add(1L);
          } else if (segment.getBlockId() == 2) {
            assertTrue(segment.equals(new BufferSegment(2, 70, 20, 0)));
            testedBlockIds.add(2L);
          }
        }
        assertEquals(2, testedBlockIds.size());
        expectedOffset.remove(10L);
      }
      if (seg.getOffset() == 500) {
        assertEquals(120, seg.getLength());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(1, bufferSegments.size());
        assertTrue(bufferSegments.get(0).equals(new BufferSegment(5, 0, 120, 0)));
        expectedOffset.remove(500L);
      }
      if (seg.getOffset() == 630) {
        assertEquals(10, seg.getLength());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(1, bufferSegments.size());
        assertTrue(bufferSegments.get(0).equals(new BufferSegment(3, 0, 10, 0)));
        expectedOffset.remove(630L);
      }
      if (seg.getOffset() == 700) {
        assertEquals(89, seg.getLength());
        List<BufferSegment> bufferSegments = seg.getBufferSegments();
        assertEquals(2, bufferSegments.size());
        Set<Long> testedBlockIds = Sets.newHashSet();
        for (BufferSegment segment : bufferSegments) {
          if (segment.getBlockId() == 4) {
            assertTrue(segment.equals(new BufferSegment(4, 0, 20, 0)));
            testedBlockIds.add(4L);
          } else if (segment.getBlockId() == 6) {
            assertTrue(segment.equals(new BufferSegment(6, 69, 20, 0)));
            testedBlockIds.add(6L);
          }
        }
        assertEquals(2, testedBlockIds.size());
        expectedOffset.remove(700L);
      }
    }
    assertTrue(expectedOffset.isEmpty());
  }

  @Test
  public void getShuffleDataPathWithRangeTest() {
    String result = ShuffleStorageUtils.getShuffleDataPathWithRange("appId", 0, 1, 3, 6);
    assertEquals("appId/0/0-2", result);
    result = ShuffleStorageUtils.getShuffleDataPathWithRange("appId", 0, 2, 3, 6);
    assertEquals("appId/0/0-2", result);
    result = ShuffleStorageUtils.getShuffleDataPathWithRange("appId", 0, 3, 3, 6);
    assertEquals("appId/0/3-5", result);
    result = ShuffleStorageUtils.getShuffleDataPathWithRange("appId", 0, 5, 3, 6);
    assertEquals("appId/0/3-5", result);
    try {
      ShuffleStorageUtils.getShuffleDataPathWithRange("appId", 0, 6, 3, 6);
      fail("shouldn't be here");
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Can't generate ShuffleData Path"));
    }
    result = ShuffleStorageUtils.getShuffleDataPathWithRange("appId", 0, 6, 3, 7);
    assertEquals("appId/0/6-8", result);
  }
}
