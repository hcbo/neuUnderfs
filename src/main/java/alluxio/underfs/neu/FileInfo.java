package alluxio.underfs.neu;



import java.io.Serializable;

public class FileInfo implements Serializable {
    private static final long serialVersionUID = -5809782572019943999L;

    public String contentHash;
    public long contentLength;
    public long lastModified;
    long offset;
    public FileInfo(){
        super();
    }

    public FileInfo(String contentHash, long contentLength, long lastModified, long offset) {
        this.contentHash = contentHash;
        this.contentLength = contentLength;
        this.lastModified = lastModified;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "FileInfo{" +
                "contentHash='" + contentHash + '\'' +
                ", contentLength=" + contentLength +
                ", lastModified=" + lastModified +
                ", offset=" + offset +
                '}';
    }
}
