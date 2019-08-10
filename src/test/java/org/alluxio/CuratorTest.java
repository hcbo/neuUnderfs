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
import java.util.List;

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
    public void pathInfoTest(){
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
    public void existsTest() {
        String path = "neu:///Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/metadata10";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/dummy/hcb"), UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));

        try {
            System.out.println(neuUnderFileSystem.exists(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mkdirsTest() throws Exception{
        String rootPath = "/Users/hcb/Documents/testFile/dummy3";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI(rootPath), UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
        String path = "neu:///Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources/14";
//        System.out.println(neuUnderFileSystem.exists(path));
//        System.out.println(neuUnderFileSystem.isDirectory(path));

        System.out.println(neuUnderFileSystem.mkdirs(path));


    }



    @Test
    public void createTest(){
        String path = "neu:///Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0/16.snapshot.alluxio.0x0000016C6BDF5582.tmp";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/Users/hcb/Documents/testFile/dummy3"),UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
        try {
            NeuFileOutputStream neuFileOutputStream = (NeuFileOutputStream)neuUnderFileSystem.create(path,null);
            neuFileOutputStream.write("haochangbo3".getBytes());
            neuFileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void openTest(){
        String path = "neu:///Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0/16.snapshot";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/Users/hcb/Documents/testFile/dummy3"),UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
        try {
            NeuFileInputStream neuFileInputStream = (NeuFileInputStream)neuUnderFileSystem.open(path,null);
            byte[] buffer = new byte[1024];
            neuFileInputStream.read(buffer);
            System.out.println(new String(buffer));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void readMetadataTest(){
        client.start();
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath("/Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0/16.snapshot");
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        System.out.println(pathInfo);
    }

    @Test
    public void isFileTest(){
        String path = "neu:///Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0/16.snapshot";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/Users/hcb/Documents/testFile/dummy3"),UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
        try {
            boolean flag = neuUnderFileSystem.isFile(path);
            System.out.println(flag);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void curatorGetChildrenTest(){
        String underPath = "/Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1";
        client.start();
        List<String>  children = null;
        try {
            children =client.getChildren().forPath(underPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(children);

    }






































}
