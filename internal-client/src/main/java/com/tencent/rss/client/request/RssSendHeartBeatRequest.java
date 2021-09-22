package com.tencent.rss.client.request;

import java.util.Set;

public class RssSendHeartBeatRequest {

  private String shuffleServerId;
  private String shuffleServerIp;
  private int shuffleServerPort;
  private long usedMemory;
  private long preAllocatedMemory;
  private long availableMemory;
  private int eventNumInFlush;
  private Set<String> tags;
  private long timeout;

  public RssSendHeartBeatRequest(String shuffleServerId, String shuffleServerIp, int shuffleServerPort, long usedMemory,
      long preAllocatedMemory, long availableMemory, int eventNumInFlush, long timeout, Set<String> tags) {
    this.shuffleServerId = shuffleServerId;
    this.shuffleServerIp = shuffleServerIp;
    this.shuffleServerPort = shuffleServerPort;
    this.usedMemory = usedMemory;
    this.preAllocatedMemory = preAllocatedMemory;
    this.availableMemory = availableMemory;
    this.eventNumInFlush = eventNumInFlush;
    this.tags = tags;
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

  public long getTimeout() {
    return timeout;
  }

  public long getUsedMemory() {
    return usedMemory;
  }

  public long getPreAllocatedMemory() {
    return preAllocatedMemory;
  }

  public long getAvailableMemory() {
    return availableMemory;
  }

  public int getEventNumInFlush() {
    return eventNumInFlush;
  }

  public Set<String> getTags() {
    return tags;
  }
}
