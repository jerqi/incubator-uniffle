package com.tencent.rss.common;

import com.google.common.collect.Lists;
import com.tencent.rss.proto.RssProtos.ShuffleData;
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
