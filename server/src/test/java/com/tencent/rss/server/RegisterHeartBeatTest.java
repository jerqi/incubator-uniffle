package com.tencent.rss.server;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.proto.CoordinatorServerGrpc.CoordinatorServerImplBase;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class RegisterHeartBeatTest {

  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();
  /**
   * This rule manages automatic graceful shutdown for the registered servers and channels at the
   * end of test.
   */
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private CoordinatorGrpcClient client;
  private RegisterHeartBeat rh;

  @Before
  public void setUp() throws Exception {
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();
    // Use a mutable service registry for later registering the service impl for each test case.
    grpcCleanup.register(InProcessServerBuilder.forName(serverName)
        .fallbackHandlerRegistry(serviceRegistry).directExecutor().build().start());
    client = new CoordinatorGrpcClient(grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    rh = new RegisterHeartBeat(new ShuffleServer(confFile), client);
  }

  private boolean sendHeartBeat(RegisterHeartBeat rh) {
    return rh.sendHeartBeat(
        "", "", 0, 0, 0, 0, 0);
  }

  @Test
  public void heartBeatTest() {
    CoordinatorServerImplBase serviceImpl =
        new CoordinatorServerImplBase() {
          @Override
          public void heartbeat(ShuffleServerHeartBeatRequest req,
              StreamObserver<ShuffleServerHeartBeatResponse> streamObserver) {
            ShuffleServerHeartBeatResponse resp = ShuffleServerHeartBeatResponse
                .newBuilder()
                .setStatus(RssProtos.StatusCode.SUCCESS)
                .build();
            streamObserver.onNext(resp);
            streamObserver.onCompleted();
          }
        };
    serviceRegistry.addService(serviceImpl);
    boolean ret = sendHeartBeat(rh);
    assertFalse(ret);

    rh.setHeartBeatTimeout(10 * 1000L);
    ret = sendHeartBeat(rh);
    assertTrue(ret);
  }

}
