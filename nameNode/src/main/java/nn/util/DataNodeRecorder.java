package nn.util;

import com.f4.proto.common.PeerInfo;
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
            BufferedReader reader = new BufferedReader(new FileReader(PropertiesReader.getPropertyAsString("nameNode.slaveConfigFilePath")));
            String slave;
            while((slave = reader.readLine()) != null){
                if(parsePeerInfo(slave) != null){
                    ALL_DATANODE.add(slave);
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

    private String getPeerInfoString(PeerInfo peerInfo) throws NullPointerException{
        return String.join(":", peerInfo.getIP(), "" + peerInfo.getPort());
    }

    public void addActiveDataNode(PeerInfo peerInfo){
        try{
            String peer = getPeerInfoString(peerInfo);
            ACTIVE_DATANODE.add(peer);
            CLIENT_MAP.put(peer, new FileOperationClient(peerInfo.getIP(), peerInfo.getPort()));
        }catch (NumberFormatException e){
            e.printStackTrace();
        }catch (NullPointerException e){
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

    public boolean isExist(PeerInfo ele){
        return ALL_DATANODE.contains(getPeerInfoString(ele));
    }

    public boolean isActive(PeerInfo ele){
        return ACTIVE_DATANODE.contains(getPeerInfoString(ele));
    }

    public PeerInfo getActiveSlave(int index){
        return parsePeerInfo(ACTIVE_DATANODE.get(index));
    }

    public int getActiveSlaveNum(){return ACTIVE_DATANODE.size();}
}
