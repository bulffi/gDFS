import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.FileServer;

import java.io.IOException;
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
        latch.await();
    }
}
