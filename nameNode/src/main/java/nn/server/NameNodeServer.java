package nn.server;

import com.f4.proto.nn.MasterGrpc;
import com.f4.proto.nn.RegisterRequest;
import com.f4.proto.nn.RegisterResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import nn.util.DataNodeRecorder;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class NameNodeServer {
    private static final Logger logger = Logger.getLogger(NameNodeServer.class.getName());

    private final int port;
    private final Server server;
    private final CountDownLatch mainThreadLatch;
    private final DataNodeRecorder recorder;

    // 初始化 查找现在能写的最大的 ID 是多少
    public NameNodeServer(ServerBuilder<?> serverBuilder, int port, CountDownLatch latch){
        this.port = port;
        mainThreadLatch = latch;
        server = serverBuilder.addService(new Master()).build();
        recorder = new DataNodeRecorder();
    }

    public NameNodeServer(int port, CountDownLatch latch){
        this(ServerBuilder.forPort(port), port,latch);
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        logger.info("server started, listening on port:" + port);
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                logger.warning("server begins to shut down");
                NameNodeServer.this.stop();
                logger.warning("server shut down");
            }
        });
        blockUntilShutdown();
    }

    private void stop(){
        if(server!=null){
            server.shutdown();
            mainThreadLatch.countDown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException{
        if(server!=null){
            server.awaitTermination();
        }
    }



    private class Master extends MasterGrpc.MasterImplBase{
        @Override
        public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
            String host = request.getHost();
            String port = request.getPort();
            String addr = String.join(":", host, port);

            if(recorder.isExist(addr) && !recorder.isActive(addr)){
                RegisterResponse.Builder builder = RegisterResponse.newBuilder().setStatus(true);
                for(int i = 0; i < DataNodeRecorder.ACTIVE_DATANODE.size(); i++){
                    builder.addPeers(recorder.getActiveSlave(i));
                }
                recorder.addActiveDataNode(addr);
                responseObserver.onNext(builder.build());
            } else{
                responseObserver.onNext(RegisterResponse.newBuilder().setStatus(false).build());
            }
            responseObserver.onCompleted();
        }
    }
}
