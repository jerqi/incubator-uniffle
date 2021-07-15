package com.tencent.rss.common;

import java.util.Objects;

/**
 * Class for partition range: [start, end]
 * Note: both inclusive
 */
public class PartitionRange implements Comparable<PartitionRange> {

  private final int start;
  private final int end;

  public PartitionRange(int start, int end) {
    if (start > end || start < 0) {
      throw new IllegalArgumentException("Illegal partition range [start, end]");
    }
    this.start = start;
    this.end = end;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  public int getPartitionNum() {
    return end - start + 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionRange that = (PartitionRange) o;
    return start == that.start && end == that.end;
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end);
  }

  @Override
  public String toString() {
    return "PartitionRange[" + start + ", " + end + ']';
  }

  @Override
  public int compareTo(PartitionRange o) {
    return this.getStart() - o.getStart();
  }
}
