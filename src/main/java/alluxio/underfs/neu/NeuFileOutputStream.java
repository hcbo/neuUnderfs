package alluxio.underfs.neu;

import alluxio.util.UnderFileSystemUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class NeuFileOutputStream extends OutputStream {
    final int BYTE_BUFFER_SIZE = 1024;
    byte[] byteBuffer = new byte[BYTE_BUFFER_SIZE];
    int pointer;

    PathInfo pathInfo;

    CuratorFramework client;

    Producer<String, byte[]> producer;


    public NeuFileOutputStream(CuratorFramework zkclient,String path) {
        this.client = zkclient;
        pathInfo = new PathInfo();
        pathInfo.name = renameFile(path);
        pathInfo.isDirectory = false;

        Properties properties = new Properties();

        try {
            properties.load(new FileReader
                    (new File("src/main/resources/kafka.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(client == null){

            String zkServers = properties.getProperty("bootstrap.servers").split(":")[0] + ":2181";

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

        producer = new KafkaProducer<String, byte[]>(properties);
    }

    @Override
    public void write(int b) throws IOException {
        byteBuffer[pointer] = (byte) b;
        pointer++;
    }


    @Override
    public void close() throws IOException {
        // 写入kafka
        String[] topicPartition = getTopicPatition(pathInfo.name);

        ProducerRecord record =
                new ProducerRecord(topicPartition[0],Integer.parseInt(topicPartition[1]),pathInfo.name, Arrays.copyOf(byteBuffer,pointer));

        Future<RecordMetadata> future = producer.send(record);
        // 下边这句代码必须有,会刷新缓存到主题.
        producer.flush();
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


        // 写元信息
        pathInfo.fileInfo.offset = recordMetadata.offset();
        pathInfo.fileInfo.contentLength = pointer;
        pathInfo.lastModified = System.currentTimeMillis();
        pathInfo.fileInfo.contentHash = UnderFileSystemUtils.approximateContentHash(pathInfo.fileInfo.contentLength, pathInfo.lastModified);
        byte[] input = SerializationUtils.serialize(pathInfo);
        try {
            client.create()
                    .creatingParentContainersIfNeeded()
                    .forPath(pathInfo.name, input);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    /**
     * filePath的几种形式
     *  /dummy/checkpoint_streaming1/commits/12
     *  /dummy/checkpoint_streaming1/metadata
     *  /dummy/checkpoint_streaming1/offsets/18
     *  /dummy/checkpoint_streaming1/sources/0/0
     *  /dummy/checkpoint_streaming1/state/0/0/.5.delta.5a88bcdc-c3b4-4ac4-b89e-089fd0648bf7.TID11.tmp
     *  /dummy/checkpoint_streaming1/state/0/0/17.delta
     *  /dummy/checkpoint_streaming1/state/0/0/16.snapshot
     *
     * @param filePath
     * @return
     */
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


    private String renameFile(String path){
        String str = path;
        if(path.endsWith("tmp")){
            int end = path.indexOf(".alluxio");
            str =  path.substring(0,end);
        }

        return str;
    }

}