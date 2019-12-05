package nn.message;

import com.f4.proto.common.PeerInfo;

public class BlockInfo {
    long blockID;
    PeerInfo dnID;
    long duplicationID;

    public BlockInfo(long blockID, PeerInfo dnID, long duplicationID) {
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

    public PeerInfo getDnID() {
        return dnID;
    }

    public void setDnID(PeerInfo dnID) {
        this.dnID = dnID;
    }

    public long getDuplicationID() {
        return duplicationID;
    }

    public void setDuplicationID(long duplicationID) {
        this.duplicationID = duplicationID;
    }
}
