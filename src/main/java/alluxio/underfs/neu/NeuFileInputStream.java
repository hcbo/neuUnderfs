package alluxio.underfs.neu;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class NeuFileInputStream extends InputStream {
    byte[] byteBuffer ;
    int pointer;


    CuratorFramework client;

    KafkaConsumer<String,byte[]> consumer;

    public NeuFileInputStream(CuratorFramework zkclient, String path,KafkaConsumer<String,byte[]> consum) {
        NeuUnderFileSystem.LOG.error("NeuFileInputStream.构造函数调用");
        this.client = zkclient;
        this.consumer = consum;

        if(client == null){
            String zkServers = "192.168.225.6:2181";
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
            this.client = CuratorFrameworkFactory.builder()
                    .connectString(zkServers)
                    .retryPolicy(retryPolicy)
                    .sessionTimeoutMs(6000)
                    .connectionTimeoutMs(3000)
                    .namespace("fileSize1")
                    .build();
            this.client.start();
        }



        // get offset from zookeeper
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        long offset = pathInfo.fileInfo.offset;

        // get message from kafka
        String[] tp = getTopicPatition(pathInfo.name);
        TopicPartition topicPartition = new TopicPartition(tp[0],Integer.parseInt(tp[1]));
        List topicPartitionList = new ArrayList<TopicPartition>();
        topicPartitionList.add(topicPartition);
        consumer.assign(topicPartitionList);
        consumer.seek(topicPartition,offset);
        ConsumerRecords<String, byte[]> records;
        while (true) {
            records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            if(!records.isEmpty()){
                break;
            }
        }
        Iterator<ConsumerRecord<String, byte[]>> iterator =records.iterator();
        ConsumerRecord<String, byte[]> record = iterator.next();
        byteBuffer = record.value();

        NeuUnderFileSystem.LOG.error("NeuFileInputStream.构造函数调用结束");
    }

    @Override
    public int read() throws IOException {
        if(pointer < byteBuffer.length){
            return byteBuffer[pointer++];

        }
        return -1;
    }

    private String[] getTopicPatition(String filePath) {
        String[] tps= new String[2];
        String[] subPaths = filePath.split("/");
        int len = subPaths.length;

        if (subPaths[len-1].contains("delta")||subPaths[len-1].contains("snapshot")){
            tps[1] = subPaths[len-2];
            tps[0] = toTopicName(subPaths,len-2);
        }else if(subPaths[len-1].contains("metadata")){
            tps[1] = Integer.toString(0);
            tps[0] = toTopicName(subPaths,len-1);
        }else if(subPaths[len-2].equals("commits")||subPaths[len-2].equals("offsets")){
            tps[1] = Integer.toString(0);
            tps[0] = toTopicName(subPaths,len-1);
        }else if(subPaths[len-3].equals("sources")){
            tps[1] = subPaths[len-2];
            tps[0] = toTopicName(subPaths,len-2);
        }
        return tps;
    }
    // kafka的主题名不能带/
    private String toTopicName(String[] subPaths,int end){
        String topicName = "";
        for (int i = 1; i < end; i++) {
            topicName = topicName+"_"+subPaths[i];
        }

        return topicName.substring(1,topicName.length());
    }
}
