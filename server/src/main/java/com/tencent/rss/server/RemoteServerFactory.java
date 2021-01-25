package com.tencent.rss.server;

import com.tencent.rss.common.rpc.GrpcServer;
import com.tencent.rss.common.rpc.ServerInterface;

public class RemoteServerFactory {

  private final ShuffleServer shuffleServer;
  private final ShuffleServerConf conf;

  public RemoteServerFactory(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
    this.conf = shuffleServer.getShuffleServerConf();
  }

  public ServerInterface getServer() {
    String type = conf.getString(ShuffleServerConf.RPC_SERVER_TYPE);
    if (type.equals(ServerTyep.GRPC.name())) {
      return new GrpcServer(conf, new GrpcService(shuffleServer));
    } else {
      throw new UnsupportedOperationException("Unsupported server type " + type);
    }
  }

  enum ServerTyep {
    GRPC
  }
}
