package alluxio.underfs.neu;

import sun.plugin2.message.Serializer;

import java.io.Serializable;

public class FileInfo implements Serializable {
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
