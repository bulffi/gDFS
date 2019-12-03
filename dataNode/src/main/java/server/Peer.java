package server;

import com.f4.proto.dn.SlaveGrpc;

/**
 * @program: gDFS
 * @description: Other data nodes
 * @author: Zijian Zhang
 * @create: 2019/12/03
 **/
public class Peer {
    private SlaveGrpc.SlaveBlockingStub stub;
    private String ip;
    private int port;

    public SlaveGrpc.SlaveBlockingStub getStub() {
        return stub;
    }

    public void setStub(SlaveGrpc.SlaveBlockingStub stub) {
        this.stub = stub;
    }

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
}
