package alluxio.underfs.neu;


import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class FileInfoSerializer implements Serializer {
    private ObjectMapper objectMapper;

    @Override
    public void configure(Map configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        byte[] ret = null;
        try{
            ret = objectMapper.writeValueAsString(data).getBytes("utf-8");
        }catch (Exception e){
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public void close() {

    }
}
