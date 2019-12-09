package nn;

import nn.server.NameNodeServer;
import nn.util.DataNodeRecorder;
import nn.util.FileOperator;
import nn.util.PropertiesReader;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class NNApp {
    private static final Logger LOGGER = Logger.getLogger(NNApp.class.getName());
    private static NameNodeServer server;
    private static boolean shallRestart = false;
    private static boolean shallStop=false;
    public static void main(String[] args){
        FileOperator fileOperator = new FileOperator();
        // 防止主线程退出
        final CountDownLatch latch = new CountDownLatch(1);

        int port = PropertiesReader.getPropertyAsInt("nameNode.port");

        port = port < 0 ? 8980 : port;
        server = new NameNodeServer(port, latch);

        // 把 Server 放到新线程里面去跑
        Thread serverThread = new Thread(){
            @Override
            public void run(){
                try {
                    while (true) {
                        shallRestart = false;
                        server.start();
                        if(shallStop){
                            return;
                        }
                        while (!shallRestart){
                            Thread.sleep(100);
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        serverThread.start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                DataNodeRecorder recorder = new DataNodeRecorder();
                while(true){
                    recorder.checkForDead();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print(">");
            String order = scanner.nextLine();
            String[] words = order.split(" ");

            if(words.length == 1){
                switch (order){
                    case "exit":
                        shallStop=true;
                        server.stop();
                        return;
                    case "help":
                        System.out.print("Usage:\n" +
                                "上传文件：upload 文件名\n" +
                                "下载文件：download 文件名 下载地址\n" +
                                "删除文件：delete 文件名\n" +
                                "追加内容：append -f 文件名 追加文件路径\n" +
                                "         append -s 文件名 追加的字符串内容\n" +
                                "退出：exit\n" +
                                "重启：restart\n");
                        break;
                    case "restart":
                        server.stop();
                        LOGGER.info("Server restarting...");
                        while (!server.isTerminated()){
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        server = new NameNodeServer(port, latch);
                        shallRestart = true;
                        break;
                    default:
                        LOGGER.warning("Unrecognized command!");
                }
            }else if(words.length > 1){
                switch (words[0]){
                    case "upload":
                        if(words.length != 2){
                            LOGGER.warning("Unrecognized command!");
                        }else {
                            fileOperator.uploadFile(words[1]);
                        }
                        break;
                    case "download":
                        if(words.length != 3){
                            LOGGER.warning("Unrecognized command!");
                        }else {
                            fileOperator.downloadFile(words[1], words[2]);
                        }
                        break;
                    case "delete":
                        if(words.length != 2){
                            LOGGER.warning("Unrecognized command!");
                        }else {
                            fileOperator.deleteFile(words[1]);
                        }
                        break;
                    case "append":
                        if(words.length == 4 && words[1].equals("-f")){
                            fileOperator.appendFileWithFile(words[2], words[3]);
                            break;
                        }
                        if(words.length >= 4 && words[1].equals("-s")){
                            fileOperator.appendFileWithString(words[2], order.substring(10 + words[2].length(), order.length()));
                            break;
                        }
                        LOGGER.warning("Unrecognized command!");
                        break;
                }
            }
        }
    }
}
