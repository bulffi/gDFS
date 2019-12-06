package nn.dao;

import com.f4.proto.common.PeerInfo;
import nn.message.BlockInfo;
import nn.util.DataNodeRecorder;
import nn.util.PropertiesReader;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class MetaDataDao {
    private static final Logger LOGGER = Logger.getLogger(MetaDataDao.class.getName());
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

    private static String HOST;
    private static int PORT;
    private static String DB_NAME;
    private static String DB_URL;
    private static String USER;
    private static String PASSWD;

    private static Connection CONN;

    public MetaDataDao(){
        this.HOST = PropertiesReader.getPropertyAsString("mysql.host");
        this.PORT = PropertiesReader.getPropertyAsInt("mysql.port");
        this.DB_NAME = PropertiesReader.getPropertyAsString("mysql.database");
        this.USER = PropertiesReader.getPropertyAsString("mysql.user");
        this.PASSWD = PropertiesReader.getPropertyAsString("mysql.passwd");
        this.DB_URL = "jdbc:mysql://" + HOST + ":" + String.valueOf(PORT) + "/" + DB_NAME + "?useSSL=false&serverTimeZone=UTC?allowPublicKeyRetrieval=true";
    }

    private Connection getConn(){
        Connection conn = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL, USER, PASSWD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public void insertFileBlock(String fileName, long blockID){
        Connection conn = getConn();
        try {
            PreparedStatement statement = conn.prepareStatement("insert into file_block values(?, ?)");
            statement.setString(1, fileName);
            statement.setLong(2, blockID);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void insertBlockDuplcation(long blockID, PeerInfo dnID, long duplicationID){
        Connection conn = getConn();
        String dnIDStr = String.join(":", dnID.getIP(), String.valueOf(dnID.getPort()));
        try{
            PreparedStatement statement = conn.prepareStatement("insert into blockDuplication values(?, ?, ?)");
            statement.setLong(1, blockID);
            statement.setString(2, dnIDStr);
            statement.setLong(3, duplicationID);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void insertFileBlockNum(String fileName, int blockNum){
        Connection conn = getConn();
        PreparedStatement statement = null;
        try {
            statement = conn.prepareStatement("insert into file_blockNum values(?, ?)");
            statement.setString(1, fileName);
            statement.setInt(2, blockNum);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public int getFileBlockNum(String fileName){
        int blockNum = 0;
        Connection conn = getConn();
        try {
            PreparedStatement statement = conn.prepareStatement("select blockNum from file_blockNum where fileName=?");
            statement.setString(1, fileName);
            ResultSet resultSet = statement.executeQuery();
            if(resultSet.next()) {
                blockNum = resultSet.getInt("blockNum");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return blockNum;
    }

    public List<BlockInfo> getAllFileBlocks(String fileName){
        Connection conn = getConn();
        List<BlockInfo> blockInfoList = new ArrayList<>();
        try{
            PreparedStatement statement = conn.prepareStatement("select file_block.blockID, dnID, duplicationID from file_block natural join blockDuplication where fileName=?");
            statement.setString(1, fileName);
            ResultSet resultSet = statement.executeQuery();
            while(resultSet.next()){
                blockInfoList.add(new BlockInfo(resultSet.getLong("blockID"),
                        DataNodeRecorder.parsePeerInfo(resultSet.getString("dnID")),
                        resultSet.getLong("duplicationID")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return blockInfoList;
    }
}
