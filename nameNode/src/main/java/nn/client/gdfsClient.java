package nn.client;

import com.f4.proto.nn.MasterGrpc;
import com.f4.proto.nn.Table;
import com.f4.proto.nn.TableContent;
import com.f4.proto.nn.TableName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class gdfsClient {
    private static final Logger logger = Logger.getLogger(gdfsClient.class.getName());

    private final ManagedChannel channel;

    // client 的所有调用都是在这个对象上进行的
    private final MasterGrpc.MasterBlockingStub blockingStub;

    public gdfsClient(ManagedChannelBuilder<?> channelBuilder){
        channel = channelBuilder.build();
        blockingStub = MasterGrpc.newBlockingStub(channel);
    }

    public gdfsClient(String host, int port){
        this(ManagedChannelBuilder.forAddress(host,port).usePlaintext());
    }

    public void shutdown() throws InterruptedException{
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args){
        gdfsClient client = new gdfsClient("localhost", 8980);
        //client.upload("hiveTest", "DDSKJLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLF");
        String tes = client.read("hiveTest");
    }

    void upload(String name, String content){
        blockingStub.updateTable(Table.newBuilder().setName(name).setContent(content).build());
    }

    String read(String name){
        TableContent content = blockingStub.readTable(TableName.newBuilder().setName(name).build());
        return content.getContent();
    }
}
