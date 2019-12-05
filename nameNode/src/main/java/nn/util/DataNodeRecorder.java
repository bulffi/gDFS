package nn.util;

import com.f4.proto.common.PeerInfo;
import nn.client.FileOperationClient;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DataNodeRecorder {
    public static List<PeerInfo> ACTIVE_DATANODE;
    public static List<PeerInfo> ALL_DATANODE;
    public static Map<PeerInfo, FileOperationClient> CLIENT_MAP;

    static{
        ALL_DATANODE = new ArrayList<>();
        ACTIVE_DATANODE = new ArrayList<>();
        CLIENT_MAP = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(PropertiesReader.getPropertyAsString("nameNode.slaveConfigFilePath")));
            String slave;
            while((slave = reader.readLine()) != null){
                PeerInfo peerInfo = parsePeerInfo(slave);
                if(peerInfo != null){
                    ALL_DATANODE.add(peerInfo);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static PeerInfo parsePeerInfo(String addr){
        String[] conf = addr.split(":");
        try {
            if (conf.length == 2) {
                int port = Integer.parseInt(conf[1]);
                return PeerInfo.newBuilder().setIP(conf[0]).setPort(port).build();
            }
        }catch (NumberFormatException e){
            e.printStackTrace();
        }
        return null;
    }

    public void addActiveDataNode(PeerInfo peerInfo){
        ACTIVE_DATANODE.add(peerInfo);
        try {
            CLIENT_MAP.put(peerInfo, new FileOperationClient(peerInfo.getIP(), peerInfo.getPort()));
        }catch (NumberFormatException e){
            e.printStackTrace();
        }
    }

    public void removeActiveDataNode(String dn){
    }

    public List<PeerInfo> getWriteDataNodes(int duplicationNum){
        if(ACTIVE_DATANODE.size() < duplicationNum){
            return null;
        }
        ACTIVE_DATANODE.sort(new Comparator<PeerInfo>() {
            @Override
            public int compare(PeerInfo peerInfo, PeerInfo t1) {
                return new Random(new Date().getTime()).nextInt(100) - 50;
            }
        });
        return ACTIVE_DATANODE.subList(0, duplicationNum);
    }

    public FileOperationClient getClient(PeerInfo dn){
        return CLIENT_MAP.get(dn);
    }

    public boolean isExist(PeerInfo ele){
        return ALL_DATANODE.contains(ele);
    }

    public boolean isActive(PeerInfo ele){
        return ACTIVE_DATANODE.contains(ele);
    }

    public PeerInfo getActiveSlave(int index){
        return ACTIVE_DATANODE.get(index);
    }

    public int getActiveSlaveNum(){return ACTIVE_DATANODE.size();}
}
