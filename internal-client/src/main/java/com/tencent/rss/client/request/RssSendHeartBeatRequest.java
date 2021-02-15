package com.tencent.rss.client.request;

public class RssSendHeartBeatRequest {

  private String shuffleServerId;
  private String shuffleServerIp;
  private int shuffleServerPort;
  private int percent;
  private long timeout;

  public RssSendHeartBeatRequest(String shuffleServerId, String shuffleServerIp, int shuffleServerPort,
      int percent, long timeout) {
    this.shuffleServerId = shuffleServerId;
    this.shuffleServerIp = shuffleServerIp;
    this.shuffleServerPort = shuffleServerPort;
    this.percent = percent;
    this.timeout = timeout;
  }

  public String getShuffleServerId() {
    return shuffleServerId;
  }

  public String getShuffleServerIp() {
    return shuffleServerIp;
  }

  public int getShuffleServerPort() {
    return shuffleServerPort;
  }

  public int getPercent() {
    return percent;
  }

  public long getTimeout() {
    return timeout;
  }
}
