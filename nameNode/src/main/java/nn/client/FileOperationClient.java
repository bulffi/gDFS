package nn.client;

import com.f4.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import nn.message.WriteStatus;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class FileOperationClient {
    private static final Logger logger = Logger.getLogger(FileOperationClient.class.getName());

    private final ManagedChannel channel;

    // client 的所有调用都是在这个对象上进行的
    private final SlaveGrpc.SlaveBlockingStub blockingStub;

    public FileOperationClient(ManagedChannelBuilder<?> channelBuilder){
        channel = channelBuilder.build();
        blockingStub = SlaveGrpc.newBlockingStub(channel);
    }

    public FileOperationClient(String host, int port){
        this(ManagedChannelBuilder.forAddress(host,port).usePlaintext());
    }

    public void shutdown() throws InterruptedException{
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private WriteReply writeToBlock(long blockID, byte[] bytes){
        logger.info("Write to block # " + blockID);

        WriteReply reply = null;
        try {
             reply = blockingStub.writeToBlock(WriteRequest.newBuilder()
                    .setBlockID(blockID)
                     .setBlock(ByteString.copyFrom(bytes))
                     .build());

            logger.info("write status: " + reply.getStatus());

        }catch (StatusRuntimeException e){
            logger.warning(e.toString());
        }
        return reply;
    }

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

    public WriteStatus upload(byte[] block){
        long blockID = getBlockID();
        WriteReply reply = writeToBlock(blockID, block);
        return new WriteStatus(reply.getStatus() == 1L, blockID);
    }


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

    public byte[] download(String dnID, long blockID){
        return readByID(blockID);
    }
}
