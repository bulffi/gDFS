package nn.server;

import com.f4.proto.common.PeerInfo;
import com.f4.proto.nn.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import nn.dao.MetaDataDao;
import nn.util.DataNodeRecorder;
import nn.util.FileOperator;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class NameNodeServer {
    private static final Logger logger = Logger.getLogger(NameNodeServer.class.getName());

    private final int port;
    private final Server server;
    private final CountDownLatch mainThreadLatch;
    private final DataNodeRecorder recorder;
    private final MetaDataDao dao;
    private FileOperator fileOperator;

    public NameNodeServer(ServerBuilder<?> serverBuilder, int port, CountDownLatch latch){
        this.port = port;
        mainThreadLatch = latch;
        server = serverBuilder.addService(new Master()).build();
        recorder = new DataNodeRecorder();
        dao = new MetaDataDao();
        fileOperator = new FileOperator();
    }

    public NameNodeServer(int port, CountDownLatch latch){
        this(ServerBuilder.forPort(port), port,latch);
    }

    public void start() throws IOException, InterruptedException {
        server.start();
        logger.info("Namenode server started, listening on port:" + port);
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

    public void stop(){
        if(server!=null){
            server.shutdown();
            mainThreadLatch.countDown();
        }
    }

    public boolean isTerminated(){
        return server.isTerminated();
    }

    private void blockUntilShutdown() throws InterruptedException{
        if(server!=null){
            server.awaitTermination();
        }
    }



    private class Master extends MasterGrpc.MasterImplBase{
        @Override
        public void register(RegisterRequest request, StreamObserver<RegisterResponse> responseObserver) {
            PeerInfo peerInfo = request.getPeer();

            if(recorder.isExist(peerInfo) && !recorder.isActive(peerInfo)){
                RegisterResponse.Builder builder = RegisterResponse.newBuilder().setStatus(true);
                for(int i = 0; i < recorder.getActiveSlaveNum(); i++){
                    builder.addPeers(recorder.getActiveSlave(i));
                }
                recorder.addActiveDataNode(peerInfo);
                responseObserver.onNext(builder.build());
                logger.info("New slave joined, address is " + peerInfo.getIP() + ":" + peerInfo.getPort());
            } else{
                responseObserver.onNext(RegisterResponse.newBuilder().setStatus(false).build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void reportDataWriteStatus(WriteReportRequest request, StreamObserver<WriteReportReply> responseObserver) {
            dao.insertBlockDuplcation(request.getLogicalBlockID(), request.getReporter(), request.getPhysicalBlockID(), request.getFileName());
            logger.info("Block#" + request.getLogicalBlockID() + " has been written!");
            responseObserver.onNext(WriteReportReply.newBuilder().setStatus(1).build());
            responseObserver.onCompleted();
        }

        @Override
        public void reportDataDeleteStatus(DeleteReportRequest request, StreamObserver<DeleteReportReply> responseObserver) {

        }

        @Override
        public StreamObserver<HeartBeatInfo> heartBeat(StreamObserver<NullReply> responseObserver) {
            return new StreamObserver<HeartBeatInfo>() {
                @Override
                public void onNext(HeartBeatInfo heartBeatInfo) {
                    PeerInfo peerInfo = heartBeatInfo.getReporter();
                    //logger.info("New heartbeat received from " + peerInfo.getIP() + ":" + peerInfo.getPort());
                    recorder.updateSlaveHeartbeatTime(peerInfo, new Date().getTime());
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.warning("Namenode have problem with heartbeat with slave ");
                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(NullReply.newBuilder().build());
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public void readTable(com.f4.proto.nn.TableName request, StreamObserver<com.f4.proto.nn.TableContent> responseObserver) {
            String content = fileOperator.readFile(request.getName());
            responseObserver.onNext(TableContent.newBuilder().setContent(content).build());
            responseObserver.onCompleted();
        }

        @Override
        public void updateTable(com.f4.proto.nn.Table request, StreamObserver<com.f4.proto.nn.Status> responseObserver) {
            fileOperator.updateFile(request.getName(), request.getContent());
            responseObserver.onNext(Status.newBuilder().build());
        }
    }
}
