package com.tencent.rss.storage.common;

import java.lang.reflect.Field;
import java.util.Objects;

public class FileBasedShuffleSegment extends ShuffleSegment implements Comparable<FileBasedShuffleSegment> {

  public static int SEGMENT_SIZE = 4 * Long.BYTES + 2 * Integer.BYTES;
  private long offset;
  private int length;
  private int uncompressLength;
  private long crc;
  private long blockId;
  private long taskAttemptId;

  public FileBasedShuffleSegment(
      long blockId,
      long offset,
      int length,
      int uncompressLength,
      long crc,
      long taskAttemptId) {
    this.offset = offset;
    this.length = length;
    this.uncompressLength = uncompressLength;
    this.crc = crc;
    this.blockId = blockId;
    this.taskAttemptId = taskAttemptId;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public long getCrc() {
    return crc;
  }

  public long getBlockId() {
    return blockId;
  }

  public void setBlockId(long blockId) {
    this.blockId = blockId;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }

  public long getTaskAttemptId() {
    return taskAttemptId;
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
        && blockId == that.blockId
        && uncompressLength == that.uncompressLength
        && taskAttemptId == that.taskAttemptId;
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
    return Objects.hash(offset, length, uncompressLength, crc, blockId);
  }

  @Override
  public String toString() {
    return "FileBasedShuffleSegment{" + "offset[" + offset + "], length[" + length
        + "], uncompressLength[" + uncompressLength + "], crc[" + crc
        + "], blockId[" + blockId + "], taskAttemptId[" + taskAttemptId + "]}";
  }
}
