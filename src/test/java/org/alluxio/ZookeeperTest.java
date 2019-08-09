package org.alluxio;

import alluxio.underfs.neu.PathMetadata;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;

public class ZookeeperTest {
    @Test
    public void writeMetadataTest(){
        ZkClient zkClient = new ZkClient("192.168.225.6:2181",5000);
        System.out.println("session created");

        PathMetadata pathMetadata = new PathMetadata(true,false,1024);

        String znodeName = "/fileSize/haochangbo2";
        byte[] input = SerializationUtils.serialize(pathMetadata);
        zkClient.createPersistent(znodeName,true);
        zkClient.writeData(znodeName,input);
    }
    @Test
    public void readMetadataTest(){
        ZkClient zkClient = new ZkClient("192.168.225.6:2181",5000);
        System.out.println("session created");
        String znodeName = "/fileSize/haochangbo2";
        PathMetadata pathMetadata = (PathMetadata) SerializationUtils.deserialize((byte[]) zkClient.readData(znodeName));
        System.out.println(pathMetadata.FileSize);

    }

}
