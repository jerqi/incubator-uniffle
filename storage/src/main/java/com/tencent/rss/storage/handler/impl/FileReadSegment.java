package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.common.BufferSegment;
import java.util.Map;

public class FileReadSegment {

  private String path;
  private long offset;
  private long length;
  private Map<Long, BufferSegment> blockIdToBufferSegment;

  public FileReadSegment(String path, long offset, long length, Map<Long, BufferSegment> blockIdToBufferSegment) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.blockIdToBufferSegment = blockIdToBufferSegment;
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

  public Map<Long, BufferSegment> getBlockIdToBufferSegment() {
    return blockIdToBufferSegment;
  }
}
