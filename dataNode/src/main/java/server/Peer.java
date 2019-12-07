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
    private SlaveGrpc.SlaveStub asyncStub;
    private String ip;

    public SlaveGrpc.SlaveStub getAsyncStub() {
        return asyncStub;
    }

    public void setAsyncStub(SlaveGrpc.SlaveStub asyncStub) {
        this.asyncStub = asyncStub;
    }

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
