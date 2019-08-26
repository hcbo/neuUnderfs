package org.alluxio;


import alluxio.underfs.neu.FileInfo;
import alluxio.underfs.neu.NeuUnderFileSystemPropertyKey;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class NormalKafka {

    @Test
    public void producerTest() {
        String topic_name = "newkafka3";
        Properties properties = new Properties();

        try {
            properties.load(new FileReader
                    (new File("src/main/resources/kafka2.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);


        ProducerRecord record =
                new ProducerRecord(topic_name, UUID.randomUUID().toString(), "ChinaNO2".getBytes());
        ProducerRecord record2 =
                new ProducerRecord(topic_name,  UUID.randomUUID().toString(), "TanwanNO1".getBytes());

        producer.send(record);
        producer.send(record2);

        // 下边这句代码必须有,会刷新缓存到主题.
        producer.flush();
    }

    @Test
    public void consumerTest() {
        String topic_name = "newkafka3";
        Properties properties = new Properties();

        try {
            properties.load(new FileReader
                    (new File("src/main/resources/kafka.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(properties);

        TopicPartition topicPartition = new TopicPartition(topic_name, 0);
        List topicPartitionList = new ArrayList<TopicPartition>();
        topicPartitionList.add(topicPartition);


        Map map = consumer.beginningOffsets(topicPartitionList);
        Map map2 = consumer.endOffsets(topicPartitionList);
        System.out.println(map.get(topicPartition));
        System.out.println(map2.get(topicPartition));


        // 为消费者指定topic和partition
        consumer.assign(topicPartitionList);
        System.out.println(consumer.position(topicPartition));

        consumer.seek(topicPartition, 5);
        System.out.println(consumer.position(topicPartition));


//        ConsumerRecords<String,byte[]> records;
//        while(true){
//            records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
//            for (ConsumerRecord<String,byte[]> record:records) {
//                System.out.println(record.key());
//                System.out.println(new String(record.value()));
//            }
//        }

    }

    @Test
    public void partitionInfoTest() {
        String topic_name = "newkafka2";
        Properties properties = new Properties();

        try {
            properties.load(new FileReader
                    (new File("src/main/resources/kafka2.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);

        List<PartitionInfo> list = producer.partitionsFor(topic_name);
        System.out.println(list.size());


//        ProducerRecord record =
//                new ProducerRecord(topic_name, 0,UUID.randomUUID().toString(),"ChinaNO1".getBytes());
//        producer.send(record);
//
//        // 下边这句代码必须有,会刷新缓存到主题.
//        producer.flush();
    }

    @Test
    public void producerTest3() {
        String topic_name = "newkafka3";
        Properties properties = new Properties();

        try {
            properties.load(new FileReader
                    (new File("src/main/resources/kafka2.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);


        ProducerRecord record;
        for (int i = 0; i < 10; i++) {
            record =
                    new ProducerRecord(topic_name, 0, String.valueOf(i), "ChinaNO1".getBytes());
            producer.send(record);
        }


        // 下边这句代码必须有,会刷新缓存到主题.
        producer.flush();
    }

    @Test
    public void consumerTest1() {
        String topic_name = "china_checkpoint_streaming2_state_0";
        Properties properties = new Properties();

        try {
            properties.load(new FileReader
                    (new File("src/main/resources/kafka2.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(properties);

        TopicPartition topicPartition = new TopicPartition(topic_name, 0);
        List topicPartitionList = new ArrayList<TopicPartition>();
        topicPartitionList.add(topicPartition);
        consumer.assign(topicPartitionList);

        consumer.seek(topicPartition,0);


        ConsumerRecords<String, byte[]> records;
        while (true) {
            records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, byte[]> record : records) {
                System.out.println(record.key());
                System.out.println(record.value().length);
            }
        }

    }

    @Test
     public void createTopicTest(){
        Properties properties = new Properties();
        //kafka的property
        properties.put("bootstrap.servers","192.168.225.6:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("enable.auto.commit",true);
        // todo group id 变化起来?
        properties.put("group.id","test10");
        properties.put("auto.offset.reset","earliest");
        properties.put("acks", "-1");
        properties.put("retries", 3);
        properties.put("buffer.memory", 33554432);


        AdminClient adminClient = AdminClient.create(properties);

        String realPath = "configTopic2";


        NewTopic newTopic = new NewTopic(realPath, 1, (short)1);

        //将包含metadata的topic消息设置为70days
        if(!realPath.contains("/")){
            Map<String, String> configs = new HashMap<>();
            configs.put("retention.ms","86400000");
            newTopic = newTopic.configs(configs);
        }

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
        // 确保topic创建成功
        KafkaFuture kafkaFuture1 = createTopicsResult.all();
        try {
            kafkaFuture1.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }



//        List<ConfigResource> resources = new ArrayList<>();
//        resources.add(new ConfigResource(ConfigResource.Type.TOPIC,realPath));
//        DescribeConfigsResult result2 = adminClient.describeConfigs(resources);
//        KafkaFuture kafkaFuture = result2.all();
//        try {
//            System.out.println(kafkaFuture.get());
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }







    }


}
