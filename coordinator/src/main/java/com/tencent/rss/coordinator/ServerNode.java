package com.tencent.rss.coordinator;

import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;

public class ServerNode implements Comparable<ServerNode> {

  private String id;
  private String ip;
  private int port;
  private long usedMemory;
  private long preAllocatedMemory;
  private long availableMemory;
  private int eventNumInFlush;
  private long timestamp;

  public ServerNode(String id, String ip, int port, long usedMemory, long preAllocatedMemory, long availableMemory,
      int eventNumInFlush) {
    this.id = id;
    this.ip = ip;
    this.port = port;
    this.usedMemory = usedMemory;
    this.preAllocatedMemory = preAllocatedMemory;
    this.availableMemory = availableMemory;
    this.eventNumInFlush = eventNumInFlush;
    this.timestamp = System.currentTimeMillis();
  }

  public static ServerNode valueOf(ShuffleServerHeartBeatRequest request) {
    ServerNode ret = new ServerNode(
        request.getServerId().getId(),
        request.getServerId().getIp(),
        request.getServerId().getPort(),
        request.getUsedMemory(),
        request.getPreAllocatedMemory(),
        request.getAvailableMemory(),
        request.getEventNumInFlush());
    return ret;
  }

  public ShuffleServerId convertToGrpcProto() {
    return ShuffleServerId.newBuilder().setId(id).setIp(ip).setPort(port).build();
  }

  public String getId() {
    return id;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public long getTimestamp() {
    return timestamp;
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

  public long getUsedMemory() {
    return usedMemory;
  }

  @Override
  public String toString() {
    return "ServerNode with id[" + id
        + "], ip[" + ip
        + "], port[" + port
        + "], usedMemory[" + usedMemory
        + "], preAllocatedMemory[" + preAllocatedMemory
        + "], availableMemory[" + availableMemory
        + "], eventNumInFlush[" + eventNumInFlush
        + "], timestamp[" + timestamp + "]";
  }

  @Override
  public int compareTo(ServerNode other) {
    if (availableMemory > other.getAvailableMemory()) {
      return -1;
    } else if (availableMemory < other.getAvailableMemory()) {
      return 1;
    } else if (eventNumInFlush > other.getEventNumInFlush()) {
      return -1;
    } else if (eventNumInFlush < other.getEventNumInFlush()) {
      return 1;
    } else if (preAllocatedMemory < other.getPreAllocatedMemory()) {
      return -1;
    } else if (preAllocatedMemory > other.getPreAllocatedMemory()) {
      return 1;
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ServerNode) {
      return id.equals(((ServerNode) obj).getId());
    }
    return false;
  }
}
