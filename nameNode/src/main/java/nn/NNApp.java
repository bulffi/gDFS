package nn;

import nn.server.NameNodeServer;
import nn.util.DataNodeRecorder;
import nn.util.FileOperator;
import nn.util.PropertiesReader;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class NNApp {
    public static void main(String[] args){
        FileOperator divider = new FileOperator(50 * 1204, 3);
        // 防止主线程退出
        final CountDownLatch latch = new CountDownLatch(1);

        // 把 Server 放到新线程里面去跑
        new Thread(){
            @Override
            public void run(){
                int port = PropertiesReader.getPropertyAsInt("nameNode.port");
                port = port < 0 ? 8980 : port;
                NameNodeServer server = new NameNodeServer(port, latch);
                try {
                    server.start();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        Scanner scanner = new Scanner(System.in);
        String order = scanner.nextLine();
        divider.uploadFile(new File(PropertiesReader.getPropertyAsString("test.uploadDir") + "/test.png"));
        divider.downloadFile("test.png", PropertiesReader.getPropertyAsString("test.downloadDir") + "/saturn.png");
    }
}
