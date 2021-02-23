package com.tencent.rss.coordinator;

import com.tencent.rss.common.rpc.GrpcServer;
import com.tencent.rss.common.rpc.ServerInterface;

public class CoordinatorRpcServerFactory {

  private final CoordinatorServer coordinatorServer;
  private final CoordinatorConf conf;

  public CoordinatorRpcServerFactory(CoordinatorServer coordinatorServer) {
    this.coordinatorServer = coordinatorServer;
    this.conf = coordinatorServer.getCoordinatorConf();
  }

  public ServerInterface getServer() {
    String type = conf.getString(CoordinatorConf.RPC_SERVER_TYPE);
    if (type.equals(ServerTyep.GRPC.name())) {
      return new GrpcServer(conf, new CoordinatorGrpcService(coordinatorServer));
    } else {
      throw new UnsupportedOperationException("Unsupported server type " + type);
    }
  }

  enum ServerTyep {
    GRPC
  }
}
