package com.tencent.rss.storage.handler.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.storage.common.BufferSegment;
import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class HDfsShuffleReadHandlerTest {

  @Test
  public void mergeSegmentsTest() {
    HdfsShuffleReadHandler handler = new HdfsShuffleReadHandler("appId", 1, 1, 1000,
        1, 10, 100, "basePath", Sets.newHashSet());

    List<FileBasedShuffleSegment> segments = Lists.newArrayList(
        new FileBasedShuffleSegment(0, 40, 0, 1));
    List<FileReadSegment> fileSegments = handler.mergeSegments("path", segments);
    assertEquals(1, fileSegments.size());
    for (FileReadSegment seg : fileSegments) {
      assertEquals(0, seg.getOffset());
      assertEquals(40, seg.getLength());
      assertEquals("path", seg.getPath());
      Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
      assertEquals(1, blockIdToBufferSegment.size());
      assertEquals(new BufferSegment(0, 40, 0), blockIdToBufferSegment.get(1L));
    }

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(0, 40, 0, 1),
        new FileBasedShuffleSegment(40, 40, 0, 2),
        new FileBasedShuffleSegment(80, 20, 0, 3));
    fileSegments = handler.mergeSegments("path", segments);
    assertEquals(1, fileSegments.size());
    for (FileReadSegment seg : fileSegments) {
      assertEquals(0, seg.getOffset());
      assertEquals(100, seg.getLength());
      assertEquals("path", seg.getPath());
      Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
      assertEquals(3, blockIdToBufferSegment.size());
      assertEquals(new BufferSegment(0, 40, 0), blockIdToBufferSegment.get(1L));
      assertEquals(new BufferSegment(40, 40, 0), blockIdToBufferSegment.get(2L));
      assertEquals(new BufferSegment(80, 20, 0), blockIdToBufferSegment.get(3L));
    }

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(0, 40, 0, 1),
        new FileBasedShuffleSegment(40, 40, 0, 2),
        new FileBasedShuffleSegment(80, 20, 0, 3),
        new FileBasedShuffleSegment(100, 20, 0, 4));
    fileSegments = handler.mergeSegments("path", segments);
    assertEquals(2, fileSegments.size());
    boolean tested = false;
    for (FileReadSegment seg : fileSegments) {
      if (seg.getOffset() == 100) {
        tested = true;
        assertEquals(20, seg.getLength());
        assertEquals("path", seg.getPath());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 20, 0), blockIdToBufferSegment.get(4L));
      }
    }
    assertTrue(tested);

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(0, 40, 0, 1),
        new FileBasedShuffleSegment(40, 40, 0, 2),
        new FileBasedShuffleSegment(80, 20, 0, 3),
        new FileBasedShuffleSegment(100, 20, 0, 4),
        new FileBasedShuffleSegment(120, 100, 0, 5));
    fileSegments = handler.mergeSegments("path", segments);
    assertEquals(2, fileSegments.size());
    tested = false;
    for (FileReadSegment seg : fileSegments) {
      if (seg.getOffset() == 100) {
        tested = true;
        assertEquals(120, seg.getLength());
        assertEquals("path", seg.getPath());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(2, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 20, 0), blockIdToBufferSegment.get(4L));
        assertEquals(new BufferSegment(20, 100, 0), blockIdToBufferSegment.get(5L));
      }
    }
    assertTrue(tested);

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(10, 40, 0, 1),
        new FileBasedShuffleSegment(80, 20, 0, 2),
        new FileBasedShuffleSegment(500, 120, 0, 3),
        new FileBasedShuffleSegment(700, 20, 0, 4));
    fileSegments = handler.mergeSegments("path", segments);
    assertEquals(3, fileSegments.size());
    Set<Long> expectedOffset = Sets.newHashSet(10L, 500L, 700L);
    for (FileReadSegment seg : fileSegments) {
      if (seg.getOffset() == 10) {
        assertEquals(90, seg.getLength());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(2, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 40, 0), blockIdToBufferSegment.get(1L));
        assertEquals(new BufferSegment(70, 20, 0), blockIdToBufferSegment.get(2L));
        expectedOffset.remove(10L);
      }
      if (seg.getOffset() == 500) {
        assertEquals(120, seg.getLength());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 120, 0), blockIdToBufferSegment.get(3L));
        expectedOffset.remove(500L);
      }
      if (seg.getOffset() == 700) {
        assertEquals(20, seg.getLength());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 20, 0), blockIdToBufferSegment.get(4L));
        expectedOffset.remove(700L);
      }
    }
    assertTrue(expectedOffset.isEmpty());

    segments = Lists.newArrayList(
        new FileBasedShuffleSegment(500, 120, 0, 5),
        new FileBasedShuffleSegment(630, 10, 0, 3),
        new FileBasedShuffleSegment(80, 20, 0, 2),
        new FileBasedShuffleSegment(10, 40, 0, 1),
        new FileBasedShuffleSegment(769, 20, 0, 6),
        new FileBasedShuffleSegment(700, 20, 0, 4));
    fileSegments = handler.mergeSegments("path", segments);
    assertEquals(4, fileSegments.size());
    expectedOffset = Sets.newHashSet(10L, 500L, 630L, 700L);
    for (FileReadSegment seg : fileSegments) {
      if (seg.getOffset() == 10) {
        assertEquals(90, seg.getLength());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(2, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 40, 0), blockIdToBufferSegment.get(1L));
        assertEquals(new BufferSegment(70, 20, 0), blockIdToBufferSegment.get(2L));
        expectedOffset.remove(10L);
      }
      if (seg.getOffset() == 500) {
        assertEquals(120, seg.getLength());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 120, 0), blockIdToBufferSegment.get(5L));
        expectedOffset.remove(500L);
      }
      if (seg.getOffset() == 630) {
        assertEquals(10, seg.getLength());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(1, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 10, 0), blockIdToBufferSegment.get(3L));
        expectedOffset.remove(630L);
      }
      if (seg.getOffset() == 700) {
        assertEquals(89, seg.getLength());
        Map<Long, BufferSegment> blockIdToBufferSegment = seg.getBlockIdToBufferSegment();
        assertEquals(2, blockIdToBufferSegment.size());
        assertEquals(new BufferSegment(0, 20, 0), blockIdToBufferSegment.get(4L));
        assertEquals(new BufferSegment(69, 20, 0), blockIdToBufferSegment.get(6L));
        expectedOffset.remove(700L);
      }
    }
    assertTrue(expectedOffset.isEmpty());
  }

}
