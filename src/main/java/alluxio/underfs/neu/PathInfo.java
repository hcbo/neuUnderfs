package alluxio.underfs.neu;

import java.io.Serializable;

public class PathInfo implements Serializable {
    public boolean isDirectory;
    public String name;
    public String owner = "hcb";
    public String group = "staff";
    public short mode = (short)420;
    public boolean isDeleted = false;
    public FileInfo fileInfo = new FileInfo();

    public PathInfo(boolean isDirectory, String name, String owner, String group, short mode, boolean isDeleted, FileInfo fileInfo) {
        this.isDirectory = isDirectory;
        this.name = name;
        this.owner = owner;
        this.group = group;
        this.mode = mode;
        this.isDeleted = isDeleted;
        this.fileInfo = fileInfo;
    }
    public PathInfo(){
        super();
    }

    @Override
    public String toString() {
        return "PathInfo{" +
                "isDirectory=" + isDirectory +
                ", name='" + name + '\'' +
                ", owner='" + owner + '\'' +
                ", group='" + group + '\'' +
                ", mode=" + mode +
                ", isDeleted=" + isDeleted +
                ", fileInfo=" + fileInfo +
                '}';
    }
}
