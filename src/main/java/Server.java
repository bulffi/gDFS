import com.f4.proto.SlaveGrpc;
import com.f4.proto.WriteReply;
import com.f4.proto.WriteRequest;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * @program: gDFS
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/11/29
 **/
public class Server {
    private static final Logger logger = Logger.getLogger(Server.class.getName());

    private io.grpc.Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = ServerBuilder.forPort(port)
                .addService(new SlaveImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                Server.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
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

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final Server server = new Server();
        server.start();
        server.blockUntilShutdown();
    }

    static class SlaveImpl extends SlaveGrpc.SlaveImplBase {
        @Override
        public void writeToBlock(WriteRequest request,
                                 StreamObserver<com.f4.proto.WriteReply> responseObserver) {
            File file = new File("/Users/zhangzijian/Projects/gDFS/src/main/resources/files/"+request.getBlockID());
            if (!file.exists()){
                try {
                    file.createNewFile();
                    FileWriter writer = new FileWriter(file);
                    writer.write(request.getContent());
                    writer.flush();
                }catch (IOException e){
                    responseObserver.onError(e);
                }
                responseObserver.onNext(WriteReply.newBuilder().setStatus(1).build());
                responseObserver.onCompleted();
                return;
            }
            responseObserver.onNext(WriteReply.newBuilder().setStatus(0).build());
            responseObserver.onCompleted();
        }
    }
}
