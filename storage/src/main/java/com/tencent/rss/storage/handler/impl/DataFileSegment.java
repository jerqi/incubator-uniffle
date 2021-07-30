package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import java.util.List;
import java.util.Set;

public class DataFileSegment extends FileSegment {

  private List<BufferSegment> bufferSegments;

  public DataFileSegment(String path, long offset, int length, List<BufferSegment> bufferSegments) {
    super(path, offset, length);
    this.bufferSegments = bufferSegments;
  }

  public List<BufferSegment> getBufferSegments() {
    return bufferSegments;
  }

  public Set<Long> getBlockIds() {
    Set<Long> blockIds = Sets.newHashSet();
    for (BufferSegment bs : bufferSegments) {
      blockIds.add(bs.getBlockId());
    }
    return blockIds;
  }
}
