package nn.client;

import com.f4.proto.dn.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import nn.message.BlockInfo;
import nn.util.DataNodeRecorder;

import java.util.Comparator;
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
        DeleteBlockRequest.Builder dbrBuilder = DeleteBlockRequest.newBuilder();
        DeleteInfo.Builder diBuilder = DeleteInfo.newBuilder();
        BlockInfo head = blockInfos.get(0);

        diBuilder.setIp(head.getDnID());

        blockInfos.sort(new Comparator<BlockInfo>() {
            @Override
            public int compare(BlockInfo blockInfo, BlockInfo t1) {
                String ip1 = blockInfo.getDnID().getIP();
                String ip2 = t1.getDnID().getIP();
                int port1 =blockInfo.getDnID().getPort();
                int port2 = t1.getDnID().getPort();

                int result = ip1.compareTo(ip2);
                if(result == 0){
                    return port1 - port2;
                }else {
                    return result;
                }
            }
        });
        for (BlockInfo blockInfo:blockInfos
             ) {
            if(blockInfo.getDnID().equals(head.getDnID())){
                diBuilder.addBlockID(BlockIDWrapper.newBuilder().setBlockID(blockInfo.getDuplicationID()).build());
            }else {
                dbrBuilder.addNodesToDelete(diBuilder.build());
                diBuilder = DeleteInfo.newBuilder();
                diBuilder.addBlockID(BlockIDWrapper.newBuilder().setBlockID(blockInfo.getDuplicationID()).build());
                diBuilder.setIp(blockInfo.getDnID());
                head = blockInfo;
            }
        }
    }
}
