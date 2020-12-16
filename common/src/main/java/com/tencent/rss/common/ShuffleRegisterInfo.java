package com.tencent.rss.common;

public class ShuffleRegisterInfo {

  private ShuffleServerInfo shuffleServerInfo;
  private int start;
  private int end;

  public ShuffleRegisterInfo(ShuffleServerInfo shuffleServerInfo, int start, int end) {
    this.shuffleServerInfo = shuffleServerInfo;
    this.start = start;
    this.end = end;
  }

  public ShuffleServerInfo getShuffleServerInfo() {
    return shuffleServerInfo;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }

  @Override
  public int hashCode() {
    return shuffleServerInfo.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ShuffleRegisterInfo) {
      return shuffleServerInfo.equals(((ShuffleRegisterInfo) obj).getShuffleServerInfo())
        && start == ((ShuffleRegisterInfo) obj).getStart()
        && end == ((ShuffleRegisterInfo) obj).getEnd();
    }
    return false;
  }
}
