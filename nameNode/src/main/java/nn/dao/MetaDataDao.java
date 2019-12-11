package nn.dao;

import com.f4.proto.common.PeerInfo;
import nn.message.BlockInfo;
import nn.util.DataNodeRecorder;
import nn.util.PropertiesReader;

import javax.swing.plaf.nimbus.State;
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

    public boolean checkExistence(String fileName){
        Connection conn = getConn();
        try{
            PreparedStatement statement = conn.prepareStatement("select * from file_blockNum where fileName=?");
            statement.setString(1, fileName);
            ResultSet resultSet = statement.executeQuery();
            if(resultSet.next()){
                return true;
            }else {
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }catch (NullPointerException e) {
            LOGGER.warning("Can't get database connection!");
            return false;
        }finally{
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void insertBlockDuplcation(long blockID, PeerInfo dnID, long duplicationID, String fileName){
        Connection conn = getConn();
        String dnIDStr = String.join(":", dnID.getIP(), String.valueOf(dnID.getPort()));
        try{
            PreparedStatement statement = conn.prepareStatement("insert into blockDuplication values(?, ?, ?, ?)");
            statement.setLong(1, blockID);
            statement.setString(2, dnIDStr);
            statement.setLong(3, duplicationID);
            statement.setString(4, fileName);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }catch (NullPointerException e) {
            LOGGER.warning("Can't get database connection!");
        }
        finally {
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
        }catch (NullPointerException e) {
            LOGGER.warning("Can't get database connection!");
        } finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void updateFileBlockNum(String fileName, int blockNum){
        Connection conn = getConn();
        try {
            PreparedStatement statement = conn.prepareStatement("update file_blockNum set blockNum=? where fileName=?");
            statement.setLong(1, blockNum);
            statement.setString(2, fileName);
            statement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }catch (NullPointerException e) {
            LOGGER.warning("Can't get database connection!");
        }
        finally {
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
        }catch (NullPointerException e) {
            LOGGER.warning("Can't get database connection!");
        }
        finally {
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
            PreparedStatement statement = conn.prepareStatement("select blockID, dnID, duplicationID from blockDuplication where fileName=?");
            statement.setString(1, fileName);
            ResultSet resultSet = statement.executeQuery();
            while(resultSet.next()){
                blockInfoList.add(new BlockInfo(resultSet.getLong("blockID"),
                        DataNodeRecorder.parsePeerInfo(resultSet.getString("dnID")),
                        resultSet.getLong("duplicationID")));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }catch (NullPointerException e) {
            LOGGER.warning("Can't get database connection!");
        }
        finally {
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

    public boolean deleteFile(String fileName){
        Connection conn = getConn();
        try{
            PreparedStatement statement1 = conn.prepareStatement("delete from blockDuplication where fileName=?");
            PreparedStatement statement2 = conn.prepareStatement("delete from file_blockNum where fileName=?");
            statement1.setString(1, fileName);
            statement2.setString(1, fileName);
            int r1 = statement1.executeUpdate();
            int r2 = statement2.executeUpdate();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }catch (NullPointerException e) {
            LOGGER.warning("Can't get database connection!");
            return false;
        }
        finally {
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public List<String> getAllFiles(){
        Connection conn = getConn();
        List<String> files = new ArrayList<>();
        try {
            Statement stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery("select distinct fileName from file_blockNum");
            while(resultSet.next()){
                files.add(resultSet.getString("fileName"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return files;
    }
}
