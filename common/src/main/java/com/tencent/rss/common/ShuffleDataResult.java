package com.tencent.rss.common;

import java.util.List;

public class ShuffleDataResult {

  private List<BufferSegment> bufferSegments;

  public ShuffleDataResult() {
    this.bufferSegments = null;
  }

  public ShuffleDataResult(List<BufferSegment> bufferSegments) {
    this.bufferSegments = bufferSegments;
  }

  public List<BufferSegment> getBufferSegments() {
    return bufferSegments;
  }

  public boolean isEmpty() {
    return bufferSegments == null || bufferSegments.isEmpty();
  }

}
