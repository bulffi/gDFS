package server;

import com.f4.proto.dn.*;
import com.f4.proto.nn.*;
import com.f4.proto.common.PeerInfo;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.LimitExceededException;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
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

    private String ip;
    private int port;
    private final Server server;
    private final CountDownLatch mainThreadLatch;
    private final AtomicLong availableBlockID;
    private final String BLOCK_PATH;

    private Master master;
    private List<Peer> peers;


    private String getPath() {
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
    // 向 name node 组册，获取初步的 peer 列表
    public FileServer(CountDownLatch latch) throws IOException {
        peers = new ArrayList<>();
        //========== 读配置文件
        Properties properties = new Properties();
        try {
            ip = InetAddress.getLocalHost().getHostAddress();
        }catch (UnknownHostException e){
            logger.error("Can not get host IP. Exit..." + " " + e.getLocalizedMessage());
            System.exit(-1);
        }
        String pathForConf = getPath() + "/conf/dataNodeConfig.properties";
        try {
            properties.load(new FileReader(new File(pathForConf)));
        }catch (IOException e1){
            try {
                properties.load(FileServer.class.getResourceAsStream("/dataNodeConfig.properties"));
            }catch (IOException e2) {
                logger.error("Fail to read dataNodeConfig.properties in " + pathForConf + " . Data node won't start.");
                throw e1;
            }
        }
        try {
            this.BLOCK_PATH = properties.get("dataNode.dataDir") + File.separator;
            this.port = Integer.parseInt((String) properties.get("dataNode.port"));
        }catch (NumberFormatException e){
            logger.error("Invalid dataNode.port config");
            throw e;
        }
        try{
            String masterIP = (String) properties.get("nameNode.ip");
            int masterPort = Integer.parseInt((String)properties.get("nameNode.port"));
            ManagedChannel channel = ManagedChannelBuilder.forAddress(masterIP,masterPort).usePlaintext().build();
            MasterGrpc.MasterBlockingStub masterStub = MasterGrpc.newBlockingStub(channel);
            MasterGrpc.MasterStub masterAsyncStub = MasterGrpc.newStub(channel);
            master = new Master();
            master.setIp(masterIP);
            master.setPort(masterPort);
            master.setStub(masterStub);
            master.setAsyncStub(masterAsyncStub);
        }catch (Exception e){
            logger.error("Can not get master node. " + e.getLocalizedMessage());
        }


        // =================== 在主机注册自己
        RegisterResponse registerResponse = master.getStub().register(RegisterRequest
                .newBuilder()
                .setPeer(PeerInfo
                        .newBuilder()
                        .setIP(ip)
                        .setPort(port)
                        .build())
                .build());
        logger.info("Try to find master with my IP: " + ip + ", PORT: "+port);
        if (registerResponse.getStatus()){
            logger.info("Successfully registered to host");
        }else {
            logger.error("Can not register to master. Exit...");
            System.exit(-1);
        }

        // =================== 开始向master做心跳
        StreamObserver<NullReply> responseObserver = new StreamObserver<NullReply>() {
            @Override
            public void onNext(NullReply nullReply) {
                logger.error("Master is suicide!");
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("Master is suicide!");
            }

            @Override
            public void onCompleted() {
                logger.error("Master is suicide!");
            }
        };
        StreamObserver<HeartBeatInfo> requestObserver = master.getAsyncStub().heartBeat(responseObserver);
        Thread reportToMaster = new Thread(){
          @Override
          public void run(){
              try {
                  while (true){
                      requestObserver.onNext(HeartBeatInfo.newBuilder()
                              .setReporter(PeerInfo.newBuilder()
                                      .setPort(port)
                                      .setIP(ip)
                                      .build())
                              .build());
                      Thread.sleep(1000);
                  }
              } catch (InterruptedException e) {
                  logger.error("Losing heartbeat to master! Shutting down...");
                  e.printStackTrace();
                  System.exit(-1);
              }
          }
        };
        reportToMaster.start();

        // ==================== 将主机告知自己的现有的节点放入连接池中
        List<PeerInfo> peerInfos = registerResponse.getPeersList();
        for (PeerInfo p : peerInfos) {
            if(!addPeer(p)){
                // TODO maybe notify the master that i can not connect?
            }
        }
        logger.info("Successfully connected to my peers");

        // ==================== 向其他的 data node 注册自己
        logger.info("Trying to find my brothers and register myself to them");
        try {
            for (Peer p : peers) {
                DataNodeReply reply = p.getStub().addNewDataNode(PeerInfo.newBuilder().setIP(ip).setPort(port).build());
                if (reply.getStatus() == 1) {
                    logger.info("I have successfully registered in node: " + p.getIp());
                } else {
                    logger.error("Fail to register in node: " + p.getIp() + " Exiting....");
                    System.exit(-1);
                }
            }
        }catch (NullPointerException e){
            logger.info("No peers to contact!");
        }
        // =================== 开通自己的 Server
        mainThreadLatch = latch;
        ServerBuilder<?> serverBuilder = ServerBuilder.forPort(this.port);
        server = serverBuilder.addService(new Slave()).build();

        // =================== 取得 available ID
        File dataDir = new File(BLOCK_PATH);
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

    private boolean addPeer(PeerInfo p) {
         /**
           * @description: 加朋友
           *
           * @param p : 盆友的信息
           *
           * @return : boolean 成功了吗
           **/
        try {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(p.getIP(),p.getPort()).usePlaintext().build();
            SlaveGrpc.SlaveBlockingStub stub = SlaveGrpc.newBlockingStub(channel);
            SlaveGrpc.SlaveStub asyncStub = SlaveGrpc.newStub(channel);
            Peer peer = new Peer();
            peer.setIp(p.getIP());
            peer.setPort(p.getPort());
            peer.setStub(stub);
            peer.setAsyncStub(asyncStub);
            peers.add(peer);
            logger.info("Get a peer in " + p.getIP() + ":" + p.getPort());
            return true;
        }catch (Exception e){
            logger.error("Can not add peer in " + p.getIP()+":"+p.getPort()+"    "+e.getLocalizedMessage());
            e.printStackTrace();
            return false;
        }

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

    private long getBlockID(){
        return availableBlockID.getAndIncrement();
    }





    // slave.proto 的真正实现类
    private class Slave extends SlaveGrpc.SlaveImplBase{
        @Override
        public void addNewDataNode(PeerInfo request,
                                   StreamObserver<DataNodeReply> responseObserver) {
            if (addPeer(request)) {
                responseObserver.onNext(DataNodeReply.newBuilder()
                        .setStatus(1).build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onNext(DataNodeReply.newBuilder()
                        .setStatus(0).build());
                responseObserver.onCompleted();
            }
        }


        @Override
        public void writeToBlock(WriteRequest request, StreamObserver<WriteReply> responseObserver)  {
            logger.info("Write file " + request.getFileName() + " logicalBlock: " + request.getLogicalBlockID());
            List<PeerInfo> temptIPS = request.getNextNodesIPsList();
            List<PeerInfo> nextIPS = new ArrayList<>(temptIPS);
            try {
                if (!(nextIPS.get(0).getIP().equals(ip) && nextIPS.get(0).getPort() == port)){
                    logger.error("This is not my IP, sth wrong with my previous");
                    return;
                }
            }catch (IndexOutOfBoundsException e){
                logger.error("IP list is empty! Should be terminated in the previous one");
                return;
            }
            long  availableBlockID = getBlockID();
            File file = new File(BLOCK_PATH + availableBlockID);
            if(file.exists()){
                responseObserver.onNext(WriteReply.newBuilder().build());
                responseObserver.onCompleted();
                logger.error("Writing Error! Block ID already used (This should never happen)");
                return;
            }
            try {
                boolean success = file.createNewFile();
                BufferedOutputStream outputStream = new BufferedOutputStream(
                        new FileOutputStream(file));
                outputStream.write(request.getBlock().toByteArray());
                outputStream.flush();
                outputStream.close();
                responseObserver.onNext(WriteReply.newBuilder().build());
                responseObserver.onCompleted();
                logger.info("Successfully write to block " + availableBlockID);
                logger.info("Report to master what I have done.");
                master.getAsyncStub().reportDataWriteStatus(
                        WriteReportRequest.newBuilder()
                                .setReporter(PeerInfo.newBuilder()
                                        .setIP(ip)
                                        .setPort(port)
                                        .build())
                                .setFileName(request.getFileName())
                                .setPhysicalBlockID(availableBlockID)
                                .setLogicalBlockID(request.getLogicalBlockID())
                                .build(), new StreamObserver<WriteReportReply>() {
                            @Override
                            public void onNext(WriteReportReply writeReportReply) {
                                if(writeReportReply.getStatus() == 0){
                                    logger.error("Master says he gets nothing! Go and check it out");
                                }
                            }

                            @Override
                            public void onError(Throwable throwable) {

                            }

                            @Override
                            public void onCompleted() {

                            }
                        });
                if (nextIPS.size() > 1){
                    //logger.info("Pass on to the next one");
                    nextIPS.remove(0);
                    PeerInfo nextIP = nextIPS.get(0);
                    //logger.info("The size of current ip list " + nextIPS.size());
                    for (Peer p : peers) {
                        //logger.info("Trying to find " + nextIP.getIP()+": "+nextIP.getPort());
                        if(p.getIp().equals(nextIP.getIP()) && p.getPort() == nextIP.getPort() ){
                            logger.info("Next peer is " + nextIP);
                            WriteRequest.Builder nextRequestBuilder = WriteRequest.newBuilder()
                                    .setBlock(request.getBlock())
                                    .setFileName(request.getFileName())
                                    .setLogicalBlockID(request.getLogicalBlockID());
                            for (PeerInfo peerInfo : nextIPS) {
                                nextRequestBuilder.addNextNodesIPs(peerInfo);
                            }
                            WriteRequest nextRequest = nextRequestBuilder.build();
                            p.getAsyncStub().writeToBlock(nextRequest, new StreamObserver<WriteReply>() {
                                @Override
                                public void onNext(WriteReply writeReply) {

                                }

                                @Override
                                public void onError(Throwable throwable) {

                                }

                                @Override
                                public void onCompleted() {

                                }
                            });
                            // WriteReply r = p.getStub().writeToBlock(nextRequest);
                           logger.info("My write is done and work is passed to next one");
                           return;
                        }
                    }
                    logger.error("Next peer is not registered " + nextIP);
                }
                logger.info("My write is done and I am the last one to write.");
            }catch (IOException e){
                logger.error(e.getLocalizedMessage());
                responseObserver.onError(e);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void readBlockByID(ReadBlockRequest request, StreamObserver<ReadBlockReply> responseObserver) {
            logger.info("master reads # " + request.getBlockID());
            File file = new File(BLOCK_PATH + request.getBlockID());
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

        @Override
        public void deleteBlockByID(DeleteBlockRequest request,
                                    StreamObserver<DeleteBlockReply> responseObserver) {
            try {
                logger.info("I am deleting block # " + request.getNodesToDeleteList().get(0).getBlockIDList());
                if(!request.getNodesToDelete(0).getIp().equals(ip)){
                    logger.error("This is not my turn! My ip is not " + request.getNodesToDelete(0).getIp());
                    return;
                }
            }catch (IndexOutOfBoundsException e){
                logger.error("The delete list is empty. My previous one has done sth wrong" + e.getLocalizedMessage());
                return;
            }
            List<DeleteInfo> deleteInfoList = request.getNodesToDeleteList();
            List<Long> blockIDToDelete = request.getNodesToDeleteList().get(0).getBlockIDList();
            List<Long> successIDs = new ArrayList<>();
            for(long id:blockIDToDelete){
                File file = new File(BLOCK_PATH + id);
                if (!file.exists()){
                    logger.error("The block # "+ id + " required to delete does not exit");
                }else {
                    boolean success = file.delete();
                    if(success){
                        logger.info("Delete block # " + id +"success");
                        successIDs.add(id);
                    }else {
                        logger.error("Delete block # "+ id + "fail");
                    }
                }
            }
            responseObserver.onNext(DeleteBlockReply.newBuilder().build());
            responseObserver.onCompleted();
            logger.info("Complete delete work.");
            logger.info("Report to master successful deleted block ids");
            DeleteReportRequest.Builder reportBuilder = DeleteReportRequest.newBuilder()
                    .setReporter(PeerInfo.newBuilder()
                            .setPort(port)
                            .setIP(ip)
                            .build());
            for (int i = 0; i < successIDs.size(); i++) {
                reportBuilder.setPhysicalBlockIDWrapper(i, intWrapper.newBuilder()
                        .setPhysicalBlockID(successIDs.get(i))
                        .build());
            }
            DeleteReportRequest reportRequest = reportBuilder.build();
            master.getAsyncStub().reportDataDeleteStatus(reportRequest, new StreamObserver<DeleteReportReply>() {
                @Override
                public void onNext(DeleteReportReply deleteReportReply) {
                    if(deleteReportReply.getStatus() == 0){
                        logger.error("Master says he has an error! Go and have a look");
                    }
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });
            if(deleteInfoList.size()>1){
                deleteInfoList.remove(0);
                logger.info("Passing to the next peer at " + deleteInfoList.get(0).getIp());

                for (Peer p : peers) {
                    if(p.getIp().equals(deleteInfoList.get(0).getIp())){
                        DeleteBlockRequest.Builder requestBuilder = DeleteBlockRequest.newBuilder();
                        for (int i = 0; i < deleteInfoList.size(); i++) {
                            requestBuilder.setNodesToDelete(i,deleteInfoList.get(i));
                        }
                        p.getAsyncStub().deleteBlockByID(requestBuilder.build(), new StreamObserver<DeleteBlockReply>() {
                            @Override
                            public void onNext(DeleteBlockReply deleteBlockReply) {

                            }

                            @Override
                            public void onError(Throwable throwable) {

                            }

                            @Override
                            public void onCompleted() {

                            }
                        });
                        logger.info("My delete is done and has passed to the next peer");
                        return;
                    }
                }
                logger.error("Next peer at: " + deleteInfoList.get(0).getIp() + "is not registered!");
            }else {
                logger.info("My delete is done and I am the last one to delete");
            }
        }

    }
}
