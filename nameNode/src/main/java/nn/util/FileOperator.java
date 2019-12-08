package nn.util;

import nn.client.FileOperationClient;
import nn.dao.MetaDataDao;
import nn.message.BlockInfo;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;
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

    public FileOperator() {
        this.blockSize = PropertiesReader.getPropertyAsInt("dataNode.blockSize");
        this.duplicationNum = PropertiesReader.getPropertyAsInt("dataNode.duplicationNum");
        block = new byte[blockSize];
        recorder = new DataNodeRecorder();
        dao = new MetaDataDao();
    }

    public void uploadFile(String filePath){
        int actualDuplicationNum = getActualDuplicationNum();
        File file = new File(filePath);
        if(!file.isFile()){
            LOGGER.warning(filePath + " is not a file!");
            return;
        }
        if(!file.exists()){
            LOGGER.warning("File " + filePath + "doesn't exist");
            return;
        }

        if(dao.checkExistence(filePath)){
            LOGGER.warning("There is already a file named " + filePath + " on gdfs, do you want to replace it?(y|n)");
            String order = new Scanner(System.in).nextLine();
            if(!order.equals("y")){
                LOGGER.info("Upload cancelled!");
                return;
            }else {
                deleteFile(filePath);
            }
        }

        try {
            BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(file));
            int index = 0;
            int read = 0;
            List<String> dns = recorder.getWriteDataNodes(actualDuplicationNum);
            while((read = buffer.read(block)) != -1){
                String dnToWrite = dns.get(0);
                FileOperationClient client = recorder.getClient(dnToWrite);
                if(read < blockSize){
                    byte[] tail = new byte[read];
                    System.arraycopy(block, 0, tail, 0, read);
                    client.writeToBlock(file.getName(), index, tail, dns);
                }else {
                    client.writeToBlock(file.getName(), index, block, dns);
                }
                index++;
            }
            buffer.close();
            dao.insertFileBlockNum(file.getName(), index);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.warning("Unknown errors!");
        }
    }

    public void downloadFile(String srcPath, String desPath){
        List<BlockInfo> blockInfoList = dao.getAllFileBlocks(srcPath);
        blockInfoList.sort(new Comparator<BlockInfo>() {
            @Override
            public int compare(BlockInfo blockInfo, BlockInfo t1) {
                int delta =  (int)(blockInfo.getBlockID() - t1.getBlockID());
                if(delta != 0){
                    return delta;
                }else {
                    return new Random(new Date().getTime()).nextInt(3) - 1;
                }
            }
        });
        int blocks = dao.getFileBlockNum(srcPath);
        int currentIndex = 0;
        try {
            BufferedOutputStream buffer = new BufferedOutputStream(new FileOutputStream(desPath, true));
            for(int i = 0; i < blockInfoList.size(); i++){
                BlockInfo blockInfo = blockInfoList.get(i);
                if(blockInfo.getBlockID() == currentIndex){
                    String peerString = DataNodeRecorder.getPeerInfoString(blockInfo.getDnID());
                    byte[] test =recorder.getClient(peerString).readByID(blockInfo.getDuplicationID());
                    LOGGER.info("Read block#" + i + " from " + peerString);
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
        }catch (Exception e){
            LOGGER.warning("Unkown errors from FileOperator");
            e.printStackTrace();
        }
    }

    public void appendFileWithString(String fileName, String content){
        int len = content.length();
        int start = 0;
        while (start + blockSize <= len){
            byte[] block = content.substring(start, start + blockSize).getBytes();
            start = start + blockSize;
        }

        if(!dao.checkExistence(fileName)){
            LOGGER.warning("The file you want to append doesn't exist!");
            return;
        }

        int actualDuplicationNum = getActualDuplicationNum();

        try {
            int index = dao.getFileBlockNum(fileName);
            List<String> dns = recorder.getWriteDataNodes(actualDuplicationNum);
            while(start < len){
                String dnToWrite = dns.get(0);
                FileOperationClient client = recorder.getClient(dnToWrite);
                if(start + blockSize > len){
                    byte[] tail = content.substring(start, len).getBytes();
                    client.writeToBlock(fileName, index, tail, dns);
                }else {
                    client.writeToBlock(fileName, index, block, dns);
                }
                index++;
            }
            dao.updateFileBlockNum(fileName, index);
        } catch (NullPointerException e){
            e.printStackTrace();
            LOGGER.warning("Client can't be empty!");
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.warning("Unknown errors!");
        }
    }

    public void appendFileWithFile(String fileName, String srcPath){
        File file = new File(srcPath);
        if(!file.isFile()){
            LOGGER.warning(srcPath + " isn't a file!");
            return;
        }
        if(!file.exists()){
            LOGGER.warning(srcPath + " doesn't exist!");
            return;
        }
        if(!dao.checkExistence(fileName)){
            LOGGER.warning("The file you want to append doesn't exist!");
            return;
        }

        int actualDuplicationNum = getActualDuplicationNum();

        try {
            BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(file));
            int index = dao.getFileBlockNum(fileName);
            int read = 0;
            List<String> dns = recorder.getWriteDataNodes(actualDuplicationNum);
            while((read = buffer.read(block)) != -1){
                String dnToWrite = dns.get(0);
                FileOperationClient client = recorder.getClient(dnToWrite);
                if(read < blockSize){
                    byte[] tail = new byte[read];
                    System.arraycopy(block, 0, tail, 0, read);
                    client.writeToBlock(file.getName(), index, tail, dns);
                }else {
                    client.writeToBlock(file.getName(), index, block, dns);
                }
                index++;
            }
            buffer.close();
            dao.updateFileBlockNum(file.getName(), index);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.warning("Unknown errors!");
        }
    }

    private int getActualDuplicationNum(){
        int actualDuplicationNum = duplicationNum;
        if(duplicationNum > recorder.getActiveSlaveNum()){
            actualDuplicationNum = recorder.getActiveSlaveNum();
            LOGGER.warning("There are not enough dataNodes to write all duplications that you specified, " +
                    "trying to write to " + actualDuplicationNum + " duplications!");
        }
        return actualDuplicationNum;
    }

    public void deleteFile(String fileName){
        dao.deleteFile(fileName);
    }
}
