package org.alluxio;


import alluxio.underfs.neu.FileInfo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class NormalKafka {
    @Test
    public void producerTest() {
        String topic_name = "newkafka2";
        Properties properties = new Properties();

        try {
            properties.load(new FileReader
                    (new File("src/main/resources/kafka.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(properties);


        ProducerRecord record =
                new ProducerRecord(topic_name, new FileInfo(), "ChinaNO1".getBytes());
        producer.send(record);

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
        String topic_name = "Users_hcb_Documents_testFile_dummy3_checkpoint_streaming1";
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
        consumer.assign(topicPartitionList);

        consumer.seek(topicPartition,5);


        ConsumerRecords<String, byte[]> records;
        while (true) {
            records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            for (ConsumerRecord<String, byte[]> record : records) {
                System.out.println(record.key());
                System.out.println(new String(record.value()));
            }
        }

    }

    @Test
    public void test1999(){
        System.out.println(System.currentTimeMillis()-864000000);
    }
}
