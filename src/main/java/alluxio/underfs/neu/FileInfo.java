package alluxio.underfs.neu;



import java.io.Serializable;

public class FileInfo implements Serializable {
    private static final long serialVersionUID = -5809782572019943999L;

    public String contentHash;
    public long contentLength;

    @Override
    public String toString() {
        return "FileInfo{" +
                "contentHash='" + contentHash + '\'' +
                ", contentLength=" + contentLength +
                ", offset=" + offset +
                '}';
    }

    public FileInfo(String contentHash, long contentLength, long offset) {
        this.contentHash = contentHash;
        this.contentLength = contentLength;
        this.offset = offset;
    }

    long offset;
    public FileInfo(){
        super();
    }




}
