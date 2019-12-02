import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.ClientDemo;
import server.FileServer;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

/**
 * @program: gDFS
 * @description: start the execution flow of data node
 * @author: Zijian Zhang
 * @create: 2019/11/30
 **/
public class dataNodeStarter {
    private static Logger logger = LogManager.getLogger(dataNodeStarter.class);
    public static void main(String[] args) throws InterruptedException, IOException {
        // 防止主线程退出
        logger.info("Try to start the data node");
        final CountDownLatch latch = new CountDownLatch(1);

        // 把 Server 放到新线程里面去跑
        Thread serverThread = new Thread(){
            @Override
            public void run(){
                FileServer server = null;
                try {
                    server = new FileServer(latch);
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    System.exit(-1);
                }
                try {
                    server.start();
                } catch (IOException | InterruptedException e) {
                    logger.error(e.getMessage());
                    System.exit(-1);
                }
            }
        };
        serverThread.start();

//        // CLI
//        Scanner scanner = new Scanner(System.in);
//        String location = "";
//        // 创建一个 client 用于和 server 通信
//        ClientDemo clientDemo = new ClientDemo("localhost",8980);
//        System.out.println("Do some upload please. And remember the blockID by yourself");
//        do {
//            // 用户输入想要上传的文件名
//            // 注意， 文件放到 /resources/uploadTest 里面去，只用输入文件名，不需要再写路径了
//            location = scanner.nextLine();
//            if (!location.equals("")){
//                // 上传
//                clientDemo.upload(location);
//            }
//        }while (!location.equals(""));
//        System.out.println("Do some download please.(enter block id and the local file name you want)");
//        long blockID = 0L;
//        do{
//            try {
//                blockID = scanner.nextLong();
//                location = scanner.next();
//            }catch (Exception e){
//                break;
//            }
//            if(!location.equals("")){
//                // 下载
//                clientDemo.downLoad(blockID,location);
//            }
//            System.out.println("Any more? q to exit");
//        }while (!location.equals(""));
//        System.out.println("We are done! Stop the server to exit.");
        latch.await();
    }
}
