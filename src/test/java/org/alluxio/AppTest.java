package org.alluxio;

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.underfs.neu.NeuUnderFileSystemPropertyKey;
import alluxio.underfs.neu.PathInfo;
import alluxio.util.SleepUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void app(){
        String path = "/Users/hcb/IdeaProjects/sparkDemo/checkpoint5/offsets/thanks123456";
        int index = path.lastIndexOf("/");
        String topicPathName = path.substring(0,index);
        System.out.println(topicPathName.replace("/","_"));
    }
    @Test
    public void test2()
    {
        String str = "haochangbo";
        byte[] buffer = new byte[100];
        buffer = str.getBytes();
        new String(buffer);

        System.out.println(new String(buffer));


    }
    @Test
    public void test4()throws Exception{
        FileInputStream fi = new FileInputStream("/Users/hcb/Documents/untitled.txt");
        Long len = fi.skip(1);
        byte[] byarr = new byte[200];
        fi.read(byarr);
        System.out.println(len);
        System.out.println(new String(byarr));
    }










    @Test
    public void test3(){
        String str = "* Returns an estimate of the number of bytes that can be read (or\n" +
                "     * skipped over) from this input stream without blocking by the next\n" +
                "     * invocation of a method for this input stream. The next invocation\n" +
                "     * might be the same thread or another thread.  A single read or skip of this\n" +
                "     * many bytes will not block, but may read or skip fewer bytes.\n" +
                "     *\n" +
                "     * <p> Note that while some implementations of {@code InputStream} will return\n" +
                "     * the total number of bytes in the stream, many will not.  It is\n" +
                "     * never correct to use the return value of this method to allocate\n" +
                "     * a buffer intended to hold all data in this stream.\n" +
                "     *\n" +
                "     * <p> A subclass' implementation of this method may choose to throw an\n" +
                "     * {@link IOException} if this input stream has been closed by\n" +
                "     * invoking the {@link #close()} method.\n" +
                "     *\n" +
                "     * <p> The {@code available} method for class {@code InputStream} always\n" +
                "     * returns {@code 0}.\n" +
                "     *\n" +
                "     * <p> This method should be overridden by subclasses.";
        String str2 = str.replace("*","").replace("\n","");
        System.out.println(str2);
    }

    @Test
    public void test98(){
        try {
            Files.write(Paths.get("/Users/hcb/Desktop/method_test/1.txt"),
                    "haochangbo\n".getBytes(), StandardOpenOption.APPEND);
            Files.write(Paths.get("/Users/hcb/Desktop/method_test/1.txt"),
                    "jinye\n".getBytes(), StandardOpenOption.APPEND);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test99(){

        List list  = new ArrayList();
        list.add("hoachaobg" );
        list.add("jinye");
        System.out.println(list.toString());
    }
    @Test
    public void test100(){
        OutputStream outputStream= null;
        try {
            outputStream = new FileOutputStream("/Users/hcb/Documents/logs/test",true);
            outputStream.write(("haochanbgo").getBytes());
            outputStream.flush();
            outputStream.close();
        } catch (Exception
                e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test101(){
        File file = new File("/Users/hcb/Documents/logs/create");
        System.out.println(file.length());
    }
    private String stripPath(String path) {
        String NEU_SCHEME = "neu://";

        if (path.startsWith(NEU_SCHEME)) {
            path = path.substring(NEU_SCHEME.length());
        }
        return new AlluxioURI(path).getPath();
    }
    @Test
    public void test102(){
        System.out.println(stripPath("neu:///Users/hcb/java/jars"));
    }
    private String fileRename(String path){
        int end = path.indexOf(".alluxio");
        return path.substring(0,end);
    }
    @Test
    public void test103(){
        System.out.println(fileRename("/Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/1/1.delta.alluxio.0x0000016C6BDF74EB.tmp"));
    }
    @Test
    public void test104(){
        String realPath = "checkpoint_streaming1/state/1224/123452";
        String underPath = "/Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0";
        boolean flag = realPath.matches(".*(/state/(\\d){1,5})/(\\d){1,5}$");
        String topicName = underPath.replace("/","_").substring(1,underPath.length());
        System.out.println(flag);
        System.out.println(topicName);
        int partitionNo = Integer.parseInt(realPath.substring(realPath.lastIndexOf("/")+1,realPath.length()));
        System.out.println(partitionNo);
    }
    @Test
    public void test105(){
        String underPath = "/Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0";
        String topicDir = underPath.substring(0,underPath.lastIndexOf("/"));
        String topicName = topicDir.replace("/","_").substring(1,topicDir.length());
        System.out.println(topicName);
    }


}
