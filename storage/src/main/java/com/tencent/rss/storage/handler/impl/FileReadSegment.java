package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import java.util.List;
import java.util.Set;

public class FileReadSegment {

  private String path;
  private long offset;
  private int length;
  private List<BufferSegment> bufferSegments;

  public FileReadSegment(String path, long offset, int length, List<BufferSegment> bufferSegments) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.bufferSegments = bufferSegments;
  }

  public String getPath() {
    return path;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
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
