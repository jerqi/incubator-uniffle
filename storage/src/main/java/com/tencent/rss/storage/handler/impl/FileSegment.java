package com.tencent.rss.storage.handler.impl;

public class FileSegment {

  private String path;
  private long offset;
  private int length;

  public FileSegment(String path, long offset, int length) {
    this.path = path;
    this.offset = offset;
    this.length = length;
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
}
