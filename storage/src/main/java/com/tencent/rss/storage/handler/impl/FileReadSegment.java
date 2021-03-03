package com.tencent.rss.storage.handler.impl;

import com.tencent.rss.storage.common.FileBasedShuffleSegment;

public class FileReadSegment {

  private FileBasedShuffleSegment fileBasedShuffleSegment;
  private String path;

  public FileReadSegment(FileBasedShuffleSegment segment, String path) {
    this.fileBasedShuffleSegment = segment;
    this.path = path;
  }

  public long getBlockId() {
    return fileBasedShuffleSegment.getBlockId();
  }

  public long getOffset() {
    return fileBasedShuffleSegment.getOffset();
  }

  public int getLength() {
    return fileBasedShuffleSegment.getLength();
  }

  public long getCrc() {
    return fileBasedShuffleSegment.getCrc();
  }

  public int getUncompressLength() {
    return fileBasedShuffleSegment.getUncompressLength();
  }

  public FileBasedShuffleSegment getFileBasedShuffleSegment() {
    return fileBasedShuffleSegment;
  }

  public String getPath() {
    return path;
  }
}
