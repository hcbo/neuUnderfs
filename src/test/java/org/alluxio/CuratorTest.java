package org.alluxio;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.neu.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

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
        FileInfo fileInfo = new FileInfo();
        PathInfo pathInfo = new PathInfo(true,"/hcb",System.currentTimeMillis());

        byte[] input = SerializationUtils.serialize(pathInfo);
        try {
            client.create()
                    .creatingParentContainersIfNeeded()
                    .forPath("/hcb", input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void curatorSetDataTest(){
        client.start();
        FileInfo fileInfo = new FileInfo();
        PathInfo pathInfo = new PathInfo(true,"/hcb",System.currentTimeMillis());

        byte[] input = SerializationUtils.serialize(pathInfo);
        try {
            client.setData()
                    .withVersion(-1)  //版本校验，与当前版本不一致则更新失败,-1则无视版本信息进行更新
                    .forPath("/hcb", input);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void existsTest() {
        String path = "/hcb";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/hcb/java"), UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));

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
        String path = "neu:///china/checkpoint_streaming1/metadata";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/china"),
                        UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
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
            output = client.getData().forPath("/china/checkpoint_streaming2/state/0/0/1.delta");
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        System.out.println(pathInfo);
    }

    @Test
    public void isFileTest(){
        String path = "neu:///china/checkpoint_streaming1/metadata";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/china"),UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
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

    @Test
    public void getDirectoryStatusTest(){
        String path = "neu:///hcb/java";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/Users/hcb/Documents/testFile/dummy3"),
                        UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
        try {
            UfsDirectoryStatus ufsDirectoryStatus = neuUnderFileSystem.getDirectoryStatus(path);
            System.out.println(ufsDirectoryStatus == null);
            System.out.println(ufsDirectoryStatus.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void isDirectoryTest(){
        String path = "neu:///hcb/java";
        NeuUnderFileSystem neuUnderFileSystem =
                new NeuUnderFileSystem(new AlluxioURI("/Users/hcb/Documents/testFile/dummy3"),
                        UnderFileSystemConfiguration.defaults(new InstancedConfiguration(new AlluxioProperties())));
        boolean flag = false;
        try {
            flag = neuUnderFileSystem.isDirectory(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(flag);
    }

    @Test
    public void curatorExistsTest(){
        String underPath = "/china";
        client.start();
        try {
            Stat stat = client.checkExists().forPath(underPath);
            System.out.println(stat.getCtime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




































}
