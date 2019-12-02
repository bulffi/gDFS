package nn.message;

public class BlockInfo {
    long blockID;
    String dnID;
    long duplicationID;

    public BlockInfo(long blockID, String dnID, long duplicationID) {
        this.blockID = blockID;
        this.dnID = dnID;
        this.duplicationID = duplicationID;
    }

    public long getBlockID() {
        return blockID;
    }

    public void setBlockID(long blockID) {
        this.blockID = blockID;
    }

    public String getDnID() {
        return dnID;
    }

    public void setDnID(String dnID) {
        this.dnID = dnID;
    }

    public long getDuplicationID() {
        return duplicationID;
    }

    public void setDuplicationID(long duplicationID) {
        this.duplicationID = duplicationID;
    }
}
