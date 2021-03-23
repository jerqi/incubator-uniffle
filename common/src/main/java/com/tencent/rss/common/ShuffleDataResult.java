package com.tencent.rss.common;

import java.util.List;

public class ShuffleDataResult {

  private byte[] data;
  private List<BufferSegment> bufferSegments;

  public ShuffleDataResult() {
    this.bufferSegments = null;
  }

  public ShuffleDataResult(byte[] data, List<BufferSegment> bufferSegments) {
    this.data = data;
    this.bufferSegments = bufferSegments;
  }

  public byte[] getData() {
    return data;
  }

  public List<BufferSegment> getBufferSegments() {
    return bufferSegments;
  }

  public boolean isEmpty() {
    return bufferSegments == null || bufferSegments.isEmpty();
  }

}
