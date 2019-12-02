package nn.message;

public class WriteStatus {
    private boolean isOK;
    private long blockID;

    public WriteStatus(boolean isOK, long blockID){
        this.isOK = isOK;
        this.blockID = blockID;
    }

    public boolean isOK() {
        return isOK;
    }

    public void setOK(boolean OK) {
        isOK = OK;
    }

    public long getBlockID() {
        return blockID;
    }

    public void setBlockID(long blockID) {
        this.blockID = blockID;
    }
}
