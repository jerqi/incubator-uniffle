package com.tencent.rss.storage.common;

import java.util.Objects;

public class FileBasedShuffleSegment extends ShuffleSegment implements Comparable<FileBasedShuffleSegment> {

  private long offset;
  private long length;
  private long crc;
  private long blockId;

  public FileBasedShuffleSegment(long offset, long length, long crc, long blockId) {
    this.offset = offset;
    this.length = length;
    this.crc = crc;
    this.blockId = blockId;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public long getCrc() {
    return crc;
  }

  public void setCrc(long crc) {
    this.crc = crc;
  }

  public long getBlockId() {
    return blockId;
  }

  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileBasedShuffleSegment that = (FileBasedShuffleSegment) o;
    return offset == that.offset
        && length == that.length
        && crc == that.crc
        && blockId == that.blockId;
  }

  @Override
  public int compareTo(FileBasedShuffleSegment fss) {
    if (this.offset > fss.getOffset()) {
      return 1;
    } else if (this.offset < fss.getOffset()) {
      return -1;
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(offset, length, crc, blockId);
  }

  @Override
  public String toString() {
    return "HDFSShuffleSegment{"
        + "offset="
        + offset
        + ", length="
        + length
        + ", crc="
        + crc
        + ", blockId='"
        + blockId
        + "'}";
  }

}
