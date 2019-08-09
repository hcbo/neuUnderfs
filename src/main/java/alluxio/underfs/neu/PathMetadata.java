package alluxio.underfs.neu;

import java.io.Serializable;

public class PathMetadata implements Serializable {
    public PathMetadata(boolean isFile, boolean isDeleted, int fileSize) {
        this.isFile = isFile;
        this.isDeleted = isDeleted;
        FileSize = fileSize;
    }

    public boolean isFile ;
    public boolean isDeleted;
    public int FileSize;
}
