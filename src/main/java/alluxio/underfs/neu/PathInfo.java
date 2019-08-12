package alluxio.underfs.neu;

import java.io.Serializable;

public class PathInfo implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;

    public boolean isDirectory;
    public String name;
    public String owner = "hcb";
    public String group = "staff";
    public short mode = (short)420;
    public boolean isDeleted = false;
    public long lastModified;
    public FileInfo fileInfo = new FileInfo();

    public PathInfo(boolean isDirectory, String name, String owner,
                    String group, short mode, boolean isDeleted,
                    long lastModified, FileInfo fileInfo) {
        this.isDirectory = isDirectory;
        this.name = name;
        this.owner = owner;
        this.group = group;
        this.mode = mode;
        this.isDeleted = isDeleted;
        this.lastModified = lastModified;
        this.fileInfo = fileInfo;
    }

    public PathInfo(boolean isDirectory, String name, long lastModified) {
        this.isDirectory = isDirectory;
        this.name = name ;
        this.lastModified = lastModified;
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
                ", lastModified=" + lastModified +
                ", fileInfo=" + fileInfo +
                '}';
    }

    public PathInfo(){
        super();
    }


}
