package com.tencent.rss.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.tencent.rss.common.CoordinatorGrpcClient;
import com.tencent.rss.proto.CoordinatorServerGrpc.CoordinatorServerImplBase;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.ShuffleServerHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.util.MutableHandlerRegistry;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
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

  private boolean register(RegisterHeartBeat rh) {
    return rh.register("", "", 0);
  }

  private boolean sendHeartBeat(RegisterHeartBeat rh) {
    return rh.sendHeartBeat("", "", 0, 0);
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
                .setStatus(StatusCode.SUCCESS)
                .build();
            streamObserver.onNext(resp);
            streamObserver.onCompleted();
          }
        };
    serviceRegistry.addService(serviceImpl);
    boolean ret = sendHeartBeat(rh);
    assertTrue(ret);
  }


  @Test
  public void heartBeatFailTest() {
    CoordinatorServerImplBase serviceImpl =
        new CoordinatorServerImplBase() {
//        @Override
//        public void registerShuffleServer(ServerRegisterRequest req,
//                                          StreamObserver<ServerRegisterResponse> streamObserver) {
//          ServerRegisterResponse resp =
//            ServerRegisterResponse.newBuilder().setStatus(StatusCode.SUCCESS).build();
//          streamObserver.onNext(resp);
//          streamObserver.onCompleted();
//        }

          @Override
          public void heartbeat(ShuffleServerHeartBeatRequest req,
              StreamObserver<ShuffleServerHeartBeatResponse> streamObserver) {
            ShuffleServerHeartBeatResponse resp = ShuffleServerHeartBeatResponse
                .newBuilder()
                .setStatus(StatusCode.INTERNAL_ERROR)
                .build();
            streamObserver.onNext(resp);
            streamObserver.onCompleted();
          }
        };
    serviceRegistry.addService(serviceImpl);

    register(rh);
    assertTrue(rh.getIsRegistered());
    rh.setMaxHeartBeatRetry(3);

    sendHeartBeat(rh);
    sendHeartBeat(rh);
    assertEquals(rh.getFailedHeartBeatCount(), 2);
    assertTrue(rh.getIsRegistered());

    sendHeartBeat(rh);
    assertFalse(rh.getIsRegistered());
    assertEquals(rh.getFailedHeartBeatCount(), 3);
  }

}
