package alluxio.underfs.neu;


import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class FileInfoDeserializer implements Deserializer {
    private ObjectMapper objectMapper;
    @Override
    public void configure(Map configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        FileInfo fileInfo = null;
        try {
            fileInfo = objectMapper.readValue(data,FileInfo.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileInfo;
    }

    @Override
    public void close() {

    }
}
