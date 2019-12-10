package nn.client;

import com.f4.proto.dn.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import nn.message.BlockInfo;
import nn.util.DataNodeRecorder;

import java.util.List;
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

    public WriteReply writeToBlock(String fileName, int logicBlockID, byte[] bytes, List<String> peers){
        logger.info("Write to block # " + logicBlockID);

        WriteReply reply = null;
        try {
            WriteRequest.Builder builder = WriteRequest.newBuilder();
            builder.setLogicalBlockID(logicBlockID).setFileName(fileName).setBlock(ByteString.copyFrom(bytes));
            for (String peer : peers
                 ) {
                builder.addNextNodesIPs(DataNodeRecorder.parsePeerInfo(peer));
            }
             reply = blockingStub.writeToBlock(builder.build());

            //logger.info("write status: " + reply.getStatus());

        }catch (StatusRuntimeException e){
            logger.warning(e.toString());
        }
        return reply;
    }

    public byte[] readByID(long blockID){
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

    public void deleteFile(List<BlockInfo> blockInfos){
        DeleteBlockRequest.Builder builder = DeleteBlockRequest.newBuilder();
        for (BlockInfo blockInfo:blockInfos
             ) {
            //builder.addNodesToDelete(DeleteInfo.newBuilder().setBlockID(blockInfo.getDuplicationID()))
        }
    }
}
