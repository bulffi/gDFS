
import com.f4.proto.SlaveGrpc;
import com.f4.proto.WriteReply;
import com.f4.proto.WriteRequest;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @program: gDFS
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/11/29
 **/
public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());
    private final ManagedChannel channel;
    private final SlaveGrpc.SlaveBlockingStub blockingStub;
    public Client(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build());
    }

    /** Construct client for accessing HelloWorld server using the existing channel. */
    public Client(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = SlaveGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /** Say hello to server. */
    public void write(String content) {
        logger.info("Will try to write " + content + " ...");
        WriteRequest request = WriteRequest.newBuilder().setBlockID(1).setContent(content).build();

        WriteReply response;
        try {
            response = blockingStub.writeToBlock(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("File upload status: " + response.getStatus());
    }
    public static void main(String[] args) throws Exception {
        // Access a service running on the local machine on port 50051
        Client client = new Client("localhost", 50051);
        try {
            String content = "This is my second file write used in gRPC";
            // Use the arg as the name to greet if provided
            if (args.length > 0) {
                content = args[0];
            }
            client.write(content);
        } finally {
            client.shutdown();
        }
    }
}
