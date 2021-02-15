package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileReadSegment {

  private String path;
  private long offset;
  private long length;
  private Map<Long, BufferSegment> blockIdToBufferSegment;
  private List<BufferSegment> bufferSegments;

  public FileReadSegment(String path, long offset, long length, Map<Long, BufferSegment> blockIdToBufferSegment) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.blockIdToBufferSegment = blockIdToBufferSegment;
    this.bufferSegments = Lists.newArrayList(blockIdToBufferSegment.values());
  }

  public String getPath() {
    return path;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
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
