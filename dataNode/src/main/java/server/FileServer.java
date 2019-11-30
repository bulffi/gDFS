package server;

import com.f4.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * @program: gDFS
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/11/30
 **/
public class FileServer {
    private static final Logger logger = Logger.getLogger(FileServer.class.getName());

    private final int port;
    private final Server server;
    private final CountDownLatch mainThreadLatch;
    private final AtomicLong availableBlockID;

    // 初始化 查找现在能写的最大的 ID 是多少
    public FileServer(ServerBuilder<?> serverBuilder,int port,CountDownLatch latch){
        File dataDir = new File("dataNode/" +
                "src/main/resources/data");
        File[] fileList = dataDir.listFiles();
        long max = 0;
        assert fileList != null;
        for (File f : fileList) {
            long tempt = Long.parseLong(f.getName());
            if (tempt > max){
                max = tempt;
            }
        }
        availableBlockID = new AtomicLong(max + 1);
        this.port = port;
        mainThreadLatch = latch;
        server = serverBuilder.addService(new Slave()).build();
    }

    public FileServer(int port, CountDownLatch latch){
        this(ServerBuilder.forPort(port), port,latch);
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        logger.info("server started, listening on port:" + port);
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                logger.warning("server begins to shut down");
                FileServer.this.stop();
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


    // slave.proto 的真正实现类
    private class Slave extends SlaveGrpc.SlaveImplBase{
        @Override
        public void writeToBlock(WriteRequest request, StreamObserver<WriteReply> responseObserver)  {
            logger.info("master writes to # " + request.getBlockID());
            File file = new File("dataNode/" +
                    "src/main/resources/data/" + request.getBlockID());
            if(file.exists()){
                responseObserver.onNext(WriteReply.newBuilder()
                        .setStatus(0).build());
                responseObserver.onCompleted();
                return;
            }
            try {
                boolean success = file.createNewFile();
                BufferedOutputStream outputStream = new BufferedOutputStream(
                        new FileOutputStream(file));
                outputStream.write(request.getBlock().toByteArray());
                outputStream.flush();
                outputStream.close();
                responseObserver.onNext(WriteReply.newBuilder()
                        .setStatus(1).build());
                responseObserver.onCompleted();
            }catch (IOException e){
                responseObserver.onError(e);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void getAvailableBlockID(QueryBlockIDRequest request, StreamObserver<QueryBlockIDReply> responseObserver) {
            logger.info("master wants available ID");
            long id = availableBlockID.getAndIncrement();
            responseObserver.onNext(QueryBlockIDReply.newBuilder()
                    .setBlockID(id).build());
            responseObserver.onCompleted();
        }

        @Override
        public void readBlockByID(ReadBlockRequest request, StreamObserver<ReadBlockReply> responseObserver) {
            logger.info("master reads # " + request.getBlockID());
            File file = new File("dataNode/" +
                    "src/main/resources/data/" + request.getBlockID());
            if (file.exists()){
                try {
                    BufferedInputStream inputStream = new BufferedInputStream(
                            new FileInputStream(file));
                    byte[] bytes = new byte[inputStream.available()];
                    int size = inputStream.read(bytes,0,inputStream.available());
                    logger.info("read " + size + "bytes from file");
                    inputStream.close();
                    responseObserver.onNext(ReadBlockReply.newBuilder()
                            .setBlock(ByteString.copyFrom(bytes)).build());
                    responseObserver.onCompleted();
                }catch (IOException e){
                    responseObserver.onError(e);
                }
            }
        }
    }


}
