package com.tencent.rss.server;

import static com.tencent.rss.server.GrpcService.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tencent.rss.common.rpc.GrpcServer;
import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.RssProtos.ShuffleData;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterRequest;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterResponse;
import com.tencent.rss.proto.ShuffleServerGrpc;
import com.tencent.rss.proto.ShuffleServerGrpc.ShuffleServerBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class GrpcShuffleServiceTest extends MetricsTestBase {

  private static final String confFile = ClassLoader.getSystemResource("server.conf").getFile();
  /**
   * This rule manages automatic graceful shutdown for the registered channel at the end of test.
   */
  @Rule
  public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private ShuffleServer server;
  private ManagedChannel inProcessChannel;
  private ShuffleServerBlockingStub stub;

  private ShuffleTaskManager mockShuffleTaskManager;

  @Before
  public void setUp() throws Exception {
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();
    server = new ShuffleServer(confFile);
    server.setServer(new GrpcServer(InProcessServerBuilder
        .forName(serverName)
        .directExecutor()
        .addService(new GrpcService(server))
        .build()));
    // Create a client channel and register for automatic graceful shutdown.
    inProcessChannel = grpcCleanup.register(
        InProcessChannelBuilder.forName(serverName).directExecutor().build());
    stub = ShuffleServerGrpc.newBlockingStub(inProcessChannel);
    server.getServer().start();
    mockShuffleTaskManager = mock(ShuffleTaskManager.class);
    server.setShuffleTaskManager(mockShuffleTaskManager);
  }

  @After
  public void tearDown() throws InterruptedException {
    /*server.getGrpcServer().shutdownNow();*/
    server.getServer().stop();
  }

  @Test
  public void registerTest() throws IOException, IllegalStateException {
    when(mockShuffleTaskManager
        .registerShuffle("", 0, 0, 0))
        .thenReturn(StatusCode.NO_BUFFER);
    when(mockShuffleTaskManager
        .registerShuffle("test", 1, 0, 10))
        .thenReturn(StatusCode.SUCCESS);

    // test default request param
    ShuffleRegisterRequest req = ShuffleRegisterRequest.newBuilder().build();
    ShuffleRegisterResponse actual = stub.registerShuffle(req);
    ShuffleRegisterResponse expected = ShuffleRegisterResponse
        .newBuilder()
        .setStatus(valueOf(StatusCode.NO_BUFFER))
        .build();
    verify(mockShuffleTaskManager, atLeastOnce()).registerShuffle(
        "", 0, 0, 0);
    assertEquals(expected, actual);

    req = ShuffleRegisterRequest.newBuilder().setAppId("test").setShuffleId(1).setStart(0).setEnd(10).build();
    actual = stub.registerShuffle(req);
    expected = ShuffleRegisterResponse
        .newBuilder()
        .setStatus(valueOf(StatusCode.SUCCESS))
        .build();
    verify(mockShuffleTaskManager, atLeastOnce()).registerShuffle(
        "test", 1, 0, 10);
    assertEquals(expected, actual);
  }

  @Test
  public void sendShuffleDataTest() throws IOException, IllegalStateException {
    ShuffleEngine mockShuffleEngine = mock(ShuffleEngine.class);
    when(mockShuffleTaskManager
        .getShuffleEngine("", "0", 0))
        .thenReturn(mockShuffleEngine);

    List<ShuffleData> shuffleDataList = new LinkedList<>();
    shuffleDataList.add(ShuffleData.newBuilder().build());
    when(mockShuffleEngine
        .write(any()))
        .thenReturn(StatusCode.SUCCESS);

    SendShuffleDataRequest req = SendShuffleDataRequest.newBuilder().build();
    SendShuffleDataResponse actual = stub.sendShuffleData(req);
    SendShuffleDataResponse expected = SendShuffleDataResponse
        .newBuilder()
        .setStatus(valueOf(StatusCode.INTERNAL_ERROR))
        .setRetMsg("No data in request")
        .build();
    assertEquals(expected, actual);

    req = SendShuffleDataRequest.newBuilder().addAllShuffleData(shuffleDataList).build();
    actual = stub.sendShuffleData(req);
    expected = SendShuffleDataResponse
        .newBuilder()
        .setStatus(valueOf(StatusCode.SUCCESS))
        .setRetMsg("OK")
        .build();
    assertEquals(expected, actual);
    verify(mockShuffleTaskManager, atLeastOnce()).getShuffleEngine(
        "", "0", 0);
    verify(mockShuffleEngine, atLeastOnce()).write(any());
  }

  @Test
  public void bufferManagerFullTest() throws IOException, IllegalStateException {
    ShuffleEngine mockShuffleEngine = mock(ShuffleEngine.class);
    when(mockShuffleTaskManager
        .getShuffleEngine("", "0", 0))
        .thenReturn(mockShuffleEngine);

    List<ShuffleData> shuffleDataList = new LinkedList<>();
    shuffleDataList.add(ShuffleData.newBuilder().build());
    when(mockShuffleEngine
        .write(any()))
        .thenReturn(StatusCode.NO_BUFFER);

    SendShuffleDataRequest req = SendShuffleDataRequest.newBuilder().addAllShuffleData(shuffleDataList).build();
    SendShuffleDataResponse actual = stub.sendShuffleData(req);
    assertEquals(valueOf(StatusCode.NO_BUFFER), actual.getStatus());
    assertTrue(actual.getRetMsg().startsWith("There is no buffer for"));
  }

  @Test
  public void commitShuffleTaskTest() throws Exception {

    when(mockShuffleTaskManager
        .commitShuffle("", 0))
        .thenReturn(StatusCode.SUCCESS);

    ShuffleCommitRequest req = ShuffleCommitRequest.newBuilder().build();
    ShuffleCommitResponse actual = stub.commitShuffleTask(req);
    ShuffleCommitResponse expected = ShuffleCommitResponse
        .newBuilder()
        .setStatus(valueOf(StatusCode.SUCCESS))
        .setRetMsg("OK")
        .build();
    assertEquals(expected, actual);
  }

}
