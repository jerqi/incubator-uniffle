package com.tencent.rss.coordinator;

import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerId;

public class ServerNode {

  private String id;
  private String ip;
  private int port;
  private int score;
  private long timestamp;

  public ServerNode(String id, String ip, int port, int score) {
    this.id = id;
    this.ip = ip;
    this.port = port;
    this.score = score;
    this.timestamp = System.currentTimeMillis();
  }

  public static ServerNode valueOf(ShuffleServerHeartBeatRequest request) {
    ServerNode ret = new ServerNode(
        request.getServerId().getId(),
        request.getServerId().getIp(),
        request.getServerId().getPort(),
        request.getScore());
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

  public int getScore() {
    return score;
  }

  public long getTimestamp() {
    return timestamp;
  }

}
