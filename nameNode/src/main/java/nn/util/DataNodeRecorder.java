package nn.util;

import nn.client.FileOperationClient;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DataNodeRecorder {
    public static List<String> ACTIVE_DATANODE;
    public static List<String> ALL_DATANODE;
    public static Map<String, FileOperationClient> CLIENT_MAP;

    static{
        ALL_DATANODE = new ArrayList<>();
        ACTIVE_DATANODE = new ArrayList<>();
        CLIENT_MAP = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader("nameNode/src/main/resources/workers"));
            String slave;
            while((slave = reader.readLine()) != null){
                String[] conf = slave.split(":");
                if(conf.length == 2) {
                    ALL_DATANODE.add(slave);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addActiveDataNode(String dn){
        ACTIVE_DATANODE.add(dn);
        String[] addr = dn.split(":");
        try {
            CLIENT_MAP.put(dn, new FileOperationClient(addr[0], Integer.parseInt(addr[1])));
        }catch (NumberFormatException e){
            e.printStackTrace();
        }
    }

    public void removeActiveDataNode(String dn){
    }

    public List<String> getWriteDataNodes(int duplicationNum){
        if(ACTIVE_DATANODE.size() < duplicationNum){
            return null;
        }
        ACTIVE_DATANODE.sort(new Comparator<String>() {
            @Override
            public int compare(String s, String t1) {
                return new Random(new Date().getTime()).nextInt(100) - 50;
            }
        });
        return ACTIVE_DATANODE.subList(0, duplicationNum);
    }

    public FileOperationClient getClient(String dn){
        return CLIENT_MAP.get(dn);
    }

    public boolean isExist(String ele){
        return ALL_DATANODE.contains(ele);
    }

    public boolean isActive(String ele){
        return ACTIVE_DATANODE.contains(ele);
    }

    public String getActiveSlave(int index){
        return ACTIVE_DATANODE.get(index);
    }
}