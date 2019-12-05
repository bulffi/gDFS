package nn.client;

import com.f4.proto.nn.MasterGrpc;
import com.f4.proto.nn.RegisterRequest;
import com.f4.proto.nn.RegisterResponse;
import com.google.protobuf.ProtocolStringList;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

//slave启动时向master注册
public class SlaveClient {
    private static final Logger logger = Logger.getLogger(FileOperationClient.class.getName());

    private String port = "7500";
    private String host = "localhost";
    private String pathForConf = "dataNode/conf/dataNodeConfig.properties";

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

//    private void register(){
//        try {
//            Properties properties = new Properties();
//            properties.load(new FileReader(new File(pathForConf)));
//
//            port = (String)properties.get("dataNode.port");
//            host = (String)properties.get("dataNode.host");
//
//            RegisterResponse response = blockingStub.register(RegisterRequest
//                    .newBuilder()
//                    .setHost(host)
//                    .setPort(port)
//                    .build());
//            ProtocolStringList list = response.getPeersList();
//            for (String message:list) {
//                System.out.println(message);
//            }
//        }catch (IOException e){
//            logger.warning("Fail to read dataNodeConfig.properties in "+ pathForConf +" . Data node won't start.");
//        }catch (NumberFormatException e){
//            logger.warning("Invalid dataNode.port config");
//        }
//    }

    public static void main(String[] args){
        //SlaveClient client = new SlaveClient("localhost", 8980);
        //client.register();
    }
}
