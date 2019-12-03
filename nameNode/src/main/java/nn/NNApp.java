package nn;

import nn.server.NameNodeServer;
import nn.util.DataNodeRecorder;
import nn.util.FileOperator;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class NNApp {
    public static void main(String[] args){
        FileOperator divider = new FileOperator(50 * 1204, 3);
        //divider.uploadFile(new File("dataNode/src/main/resources/uploadTest/test.png"));
        //divider.downloadFile("test.png", "dataNode/src/main/resources/downloadTest/saturn.png");
        // 防止主线程退出
        final CountDownLatch latch = new CountDownLatch(1);

        // 把 Server 放到新线程里面去跑
        new Thread(){
            @Override
            public void run(){
                NameNodeServer server = new NameNodeServer(8980, latch);
                try {
                    server.start();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }
}
