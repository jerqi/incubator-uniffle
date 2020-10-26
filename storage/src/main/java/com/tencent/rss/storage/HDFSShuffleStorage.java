package com.tencent.rss.storage;

import com.tencent.rss.proto.RssProtos;

import java.util.Iterator;
import java.util.List;

public class HDFSShuffleStorage implements ShuffleStorage {
  public List<ShuffleSegment> write(List<RssProtos.ShuffleBlock> shuffleBlocks) {
    return null;
  }

  public Iterator<ShuffleSegment> next() {
    return null;
  }

  public boolean close() {
    return true;
  }
}
