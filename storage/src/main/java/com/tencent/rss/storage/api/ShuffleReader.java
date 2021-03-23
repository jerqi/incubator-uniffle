package com.tencent.rss.storage.api;

import com.tencent.rss.storage.common.FileBasedShuffleSegment;
import java.io.IOException;
import java.util.List;

public interface ShuffleReader {

  byte[] readData(long offset, int length);

  List<FileBasedShuffleSegment> readIndex(int limit) throws IOException, IllegalStateException;
}
