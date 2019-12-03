package server;

import com.f4.proto.dn.*;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.*;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @program: gDFS
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/11/30
 **/
public class FileServer {
    private static final Logger logger = LogManager.getLogger(FileServer.class);

    private final int port;
    private final Server server;
    private final CountDownLatch mainThreadLatch;
    private final AtomicLong availableBlockID;

    private String getPath()
    {
        String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        if(System.getProperty("os.name").contains("dows"))
        {
            path = path.substring(1,path.length());
        }
        if(path.contains("jar"))
        {
            path = path.substring(0,path.lastIndexOf("."));
            return path.substring(0,path.lastIndexOf("/"));
        }
        return path.replace("target/classes/", "");
    }


    // 初始化 查找现在能写的最大的 ID 是多少
    public FileServer(CountDownLatch latch) throws IOException {
        Properties properties = new Properties();
        String pathForConf = getPath() + "/conf/dataNodeConfig.properties";
        try {
            properties.load(new FileReader(new File(pathForConf)));
        }catch (IOException e){
            logger.error("Fail to read dataNodeConfig.properties in "+ pathForConf +" . Data node won't start.");
            throw e;
        }
        try {
            this.port = Integer.parseInt((String) properties.get("dataNode.port"));
        }catch (NumberFormatException e){
            logger.error("Invalid dataNode.port config");
            throw e;
        }
        mainThreadLatch = latch;
        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(this.port);
        server = serverBuilder.addService(new Slave()).build();
        File dataDir = new File((String)properties.get("dataNode.dataDir"));
        if(!dataDir.exists()){
            logger.error("Please create data dir first: " + dataDir.getName());
            throw new FileNotFoundException("Data dir not found.");
        }
        File[] fileList = dataDir.listFiles();
        long max = 0;
        assert fileList != null;
        try {
            for (File f : fileList) {
                long tempt = Long.parseLong(f.getName());
                if (tempt > max){
                    max = tempt;
                }
            }
        }catch (NumberFormatException e){
            logger.error("Data dir contains illegal file name: " + e.getMessage());
            throw e;
        }
        logger.info("There is already " + max +" blocks in the data dir. Start from block #" + (max+1)) ;
        availableBlockID = new AtomicLong(max + 1);
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        logger.info("server started, listening on port:" + port);
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run(){
                logger.warn("server begins to shut down");
                FileServer.this.stop();
                logger.warn("server shut down");
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
