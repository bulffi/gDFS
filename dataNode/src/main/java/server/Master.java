package server;

import com.f4.proto.nn.MasterGrpc;

/**
 * @program: gDFS
 * @description:
 * @author: Zijian Zhang
 * @create: 2019/12/03
 **/
public class Master {
    String ip;
    int port;
    MasterGrpc.MasterBlockingStub stub;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public MasterGrpc.MasterBlockingStub getStub() {
        return stub;
    }

    public void setStub(MasterGrpc.MasterBlockingStub stub) {
        this.stub = stub;
    }
}
