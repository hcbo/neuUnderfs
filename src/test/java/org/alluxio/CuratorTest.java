package org.alluxio;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.neu.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.io.IOException;

public class CuratorTest {
    CuratorFramework client;
    public CuratorTest() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        client = CuratorFrameworkFactory.builder()
                .connectString("kafka:2181")
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(6000)
                .connectionTimeoutMs(3000)
                .namespace("fileSize1")
                .build();

    }

    @Test
    public void test1(){
        client.start();
        System.out.println("connected");
        PathMetadata pathMetadata = new PathMetadata(true,true,1024);
        byte[] input = SerializationUtils.serialize(pathMetadata);
        try {
            client.create()
                    .creatingParentContainersIfNeeded()
                    .forPath("/recursive/third", input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void readTest(){
        client.start();
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath("/recursive/forth");
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        System.out.println(pathInfo);

    }

    @Test
    public void test2(){
        client.start();
        System.out.println("connected");
        FileInfo fileInfo = new FileInfo("contentHashValue",837434L,39323L,9876L);
        PathInfo pathInfo = new PathInfo(true,"java/jars","hcb","staff",(short)420,false,fileInfo);

        byte[] input = SerializationUtils.serialize(pathInfo);
        try {
            client.create()
                    .creatingParentContainersIfNeeded()
                    .forPath("/recursive/forth", input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test3(){
        String path = "neu:///Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/metadata11.alluxio.0x0000016C6BDF5582.tmp";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/dummy/hcb"),UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
        try {
            NeuFileOutputStream neuFileOutputStream = (NeuFileOutputStream)neuUnderFileSystem.create(path,null);
            neuFileOutputStream.write("haochangbo3".getBytes());
            neuFileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    @Test
    public void readTest2(){
        client.start();
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath("/Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/metadata11");
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        System.out.println(pathInfo);

    }



}