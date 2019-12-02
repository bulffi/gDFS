package nn.client;

import com.f4.proto.nn.MasterGrpc;
import com.f4.proto.nn.RegisterRequest;
import com.f4.proto.nn.RegisterResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SlaveClient {
    private static final Logger logger = Logger.getLogger(FileOperationClient.class.getName());

    private final ManagedChannel channel;

    // client 的所有调用都是在这个对象上进行的
    private final MasterGrpc.MasterBlockingStub blockingStub;

    public SlaveClient(ManagedChannelBuilder<?> channelBuilder){
        channel = channelBuilder.build();
        blockingStub = MasterGrpc.newBlockingStub(channel);
    }

    public SlaveClient(String host, int port){
        this(ManagedChannelBuilder.forAddress(host,port).usePlaintext());
    }

    public void shutdown() throws InterruptedException{
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void register(){
        try {
            InetAddress addr = InetAddress.getLocalHost();
            RegisterResponse response = blockingStub.register(RegisterRequest
                    .newBuilder()
                    .setHost(addr.getHostName())
                    .setIp(addr.getHostAddress())
                    .build());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        SlaveClient client = new SlaveClient("localhost", 8090);
        client.register();
    }
}
