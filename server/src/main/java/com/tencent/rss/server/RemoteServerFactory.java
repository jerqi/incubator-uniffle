package com.tencent.rss.server;

public class RemoteServerFactory {
  enum ServerTyep {
    GRPC
  }

  ShuffleServer shuffleServer;
  ShuffleServerConf conf;

  public RemoteServerFactory(ShuffleServer shuffleServer) {
    this.shuffleServer = shuffleServer;
    this.conf = shuffleServer.getShuffleServerConf();
  }

  ServerInterface getServer() {
    String type = conf.getString(ShuffleServerConf.SERVER_TYPE);
    if (type.equals(ServerTyep.GRPC.name())) {
      return new GrpcServer(conf, new GrpcService(shuffleServer));
    } else {
      throw new UnsupportedOperationException("Unsupported server type " + type);
    }
  }
}
