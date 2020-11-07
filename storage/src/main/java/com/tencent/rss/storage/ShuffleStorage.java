package com.tencent.rss.storage;

import com.tencent.rss.proto.RssProtos.ShuffleBlock;

import java.util.Iterator;
import java.util.List;

public interface ShuffleStorage {

    List<ShuffleSegment> write(List<ShuffleBlock> shuffleBlocks);

    Iterator<ShuffleSegment> next();

    boolean close();
}
