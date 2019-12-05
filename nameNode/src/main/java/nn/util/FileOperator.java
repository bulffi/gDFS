package nn.util;

import com.f4.proto.common.PeerInfo;
import nn.client.FileOperationClient;
import nn.dao.MetaDataDao;
import nn.message.BlockInfo;

import java.io.*;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

public class FileOperator {
    private static final Logger LOGGER = Logger.getLogger(FileOperator.class.getName());
    private static String HOST;
    private static int PORT;

    private int blockSize;
    private int duplicationNum;
    private byte[] block;
    private final DataNodeRecorder recorder;
    private MetaDataDao dao;

    public FileOperator(int blockSize, int duplicationNum) {
        this.blockSize = blockSize;
        this.duplicationNum = duplicationNum;
        block = new byte[blockSize];
        recorder = new DataNodeRecorder();
        dao = new MetaDataDao();
    }

    public void uploadFile(File file){
        try {
            BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(file));
            int index = 0;
            List<PeerInfo> dns = recorder.getWriteDataNodes(duplicationNum);
            while(buffer.read(block) != -1){
                dao.insertFileBlock(file.getName(), index);
                PeerInfo dnToWrite = dns.get(0);
                FileOperationClient client = recorder.getClient(dnToWrite);
                client.writeToBlock(file.getName(), index, block, dns);
                index++;
            }
            dao.insertFileBlockNum(file.getName(), index);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void downloadFile(String srcPath, String desPath){
        List<BlockInfo> blockInfoList = dao.getAllFileBlocks(srcPath);
        blockInfoList.sort(new Comparator<BlockInfo>() {
            @Override
            public int compare(BlockInfo blockInfo, BlockInfo t1) {
                return (int)(blockInfo.getBlockID() - t1.getBlockID());
            }
        });
        int blocks = dao.getFileBlockNum(srcPath);
        int currentIndex = 0;
        try {
            BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream(desPath, true));
            for(int i = 0; i < blockInfoList.size(); i++){
                BlockInfo blockInfo = blockInfoList.get(i);
                if(blockInfo.getBlockID() == currentIndex){
                    byte[] test =recorder.getClient(blockInfo.getDnID()).readByID(blockInfo.getDuplicationID());
                    buffer.write(test);
                    currentIndex++;
                }
            }
            buffer.flush();
            buffer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
