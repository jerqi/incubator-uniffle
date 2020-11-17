package com.tencent.rss.server;

import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.ShuffleServerGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class ShuffleServer {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServer.class);

    private Server server;

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        if (!SessionManager.instace().init()) {
            // log fatal
            System.exit(1);
        }

        if (!BufferManager.instance().init()) {
            // log fatal
            System.exit(1);
        }

        final ShuffleServer server = new ShuffleServer();
        server.start();
        server.blockUntilShutdown();
    }

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;

        server = ServerBuilder.forPort(port)
                .addService(new ShuffleServerImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                logger.info("*** shutting down gRPC server since JVM is shutting down");
                try {
                    ShuffleServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                logger.info("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class ShuffleServerImpl extends ShuffleServerGrpc.ShuffleServerImplBase {

        @Override
        public void sendShuffleData(SendShuffleDataRequest req,
                StreamObserver<SendShuffleDataResponse> responseObserver) {
            SendShuffleDataResponse reply = null;
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void commitShuffleTask(ShuffleCommitRequest request,
                StreamObserver<ShuffleCommitResponse> responseObserver) {
            ShuffleCommitResponse reply = null;
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
