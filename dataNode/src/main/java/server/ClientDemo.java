package server;

import com.f4.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.*;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @program: gDFS
 * @description: 一个小的 client demo，没有分块，也没有备份
 * @author: Zijian Zhang
 * @create: 2019/11/30
 **/


public class ClientDemo {
    // 参看文档，写法都是固定的
    // 后期可以考虑换成异步接口
    private static final Logger logger = Logger.getLogger(ClientDemo.class.getName());

    private final ManagedChannel channel;

    // client 的所有调用都是在这个对象上进行的
    private final SlaveGrpc.SlaveBlockingStub blockingStub;

    public ClientDemo(ManagedChannelBuilder<?> channelBuilder){
        channel = channelBuilder.build();
        blockingStub = SlaveGrpc.newBlockingStub(channel);
    }

    public ClientDemo(String host, int port){
        this(ManagedChannelBuilder.forAddress(host,port).usePlaintext());
    }

    public void shutdown() throws InterruptedException{
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }


    // 写到一个块中
    private void writeToBlock(long blockID, byte[] bytes){
        logger.info("write to block # " + blockID);
        try {
            WriteReply reply = blockingStub.writeToBlock(WriteRequest.newBuilder()
                    .setBlockID(blockID).setBlock(ByteString.copyFrom(bytes)).build());
            logger.info("write status: " + reply.getStatus());
        }catch (StatusRuntimeException e){
            logger.warning(e.toString());
        }
    }

    // 查查能往哪里写
    private long getBlockID(){
        logger.info("Get block ID from data node");
        try {
            QueryBlockIDReply reply = blockingStub.getAvailableBlockID(QueryBlockIDRequest.newBuilder()
                    .build());
            logger.info("File is stored in block # " + reply.getBlockID());
            return reply.getBlockID();
        }catch (StatusRuntimeException e){
            logger.warning(e.toString());
        }
        return -1;
    }

    // 读
    private byte[] readByID(long blockID){
        logger.info("Read from block # " + blockID);
        try {
            ReadBlockReply readBlockReply = blockingStub.readBlockByID(ReadBlockRequest.newBuilder()
                    .setBlockID(blockID).build());
            return readBlockReply.getBlock().toByteArray();
        }catch (StatusRuntimeException e){
            logger.warning(e.toString());
        }
        return new byte[]{};
    }


    // 对上面三个方法做包装之后提供能简明的接口
    public void upload(String localLocation) throws IOException {
        BufferedInputStream inputStream = new BufferedInputStream(
                new FileInputStream(
                        new File("dataNode/" +
                                "src/main/resources/uploadTest/" + localLocation)));
        int length = inputStream.available();
        byte[] block = new byte[length];
        int readSize = inputStream.read(block,0,length);
        logger.info("read " + readSize + "bytes from file");
        long blockID = getBlockID();
        writeToBlock(blockID,block);
    }


    // 对那三个方法的包装
    public void downLoad(long blockID, String localName) throws IOException {
        byte[] block = readByID(blockID);
        BufferedOutputStream outputStream = new BufferedOutputStream(
                new FileOutputStream(
                        new File("dataNode/" +
                                "src/main/resources/downloadTest/" + localName)));
        outputStream.write(block);
        outputStream.flush();
        outputStream.close();
    }
}
