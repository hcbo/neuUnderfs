/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.neu;

import alluxio.AlluxioURI;
import alluxio.underfs.*;
import alluxio.underfs.options.*;
import alluxio.util.network.NetworkAddressUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * neu {@link UnderFileSystem} implementation for tutorial.
 */
@ThreadSafe
public class NeuUnderFileSystem extends ConsistentUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(NeuUnderFileSystem.class);

  public static final String NEU_SCHEME = "neu://";

  public CuratorFramework client;

  AdminClient adminClient ;

  KafkaProducer<String, byte[]> producer ;

  String rootPath ;


  /**
   * Constructs a new {@link NeuUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf UFS configuration
   */
  public NeuUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration conf) {


      super(uri, conf);
      PropertyConfigurator.configure(getLog4jProps());
      LOG.error("NeuUnderFileSystem 构造方法开始");
    // 本地mount的路径,例如/Users/hcb/Documents/testFile/String3
      this.rootPath = uri.getPath();

      RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
      client = CuratorFrameworkFactory.builder()
            .connectString(mUfsConf.get(NeuUnderFileSystemPropertyKey.ZK_SERVERS))
            .retryPolicy(retryPolicy)
            .sessionTimeoutMs(6000)
            .connectionTimeoutMs(3000)
            .namespace("fileSize1")
            .build();
      client.start();

      //写入rootPath 元信息到zookeeper
      FileInfo fileInfo = new FileInfo();
      PathInfo pathInfo = new PathInfo(true,rootPath,"hcb","staff",(short)420,false,fileInfo);

      byte[] input = SerializationUtils.serialize(pathInfo);
      try {
          client.create()
                  .creatingParentContainersIfNeeded()
                  .forPath(rootPath, input);
      } catch (Exception e) {
          e.printStackTrace();
      }
      //kafka的property
      Properties properties = new Properties();
      properties.put("bootstrap.servers",mUfsConf.get(NeuUnderFileSystemPropertyKey.KAFKA_SERVERS));
      properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
      properties.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
      properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
      properties.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
      properties.put("enable.auto.commit",true);
      // todo group id 变化起来?
      properties.put("group.id","test9");
      properties.put("auto.offset.reset","earliest");
      properties.put("acks", "-1");
      properties.put("retries", 3);
      properties.put("buffer.memory", 33554432);


      adminClient = AdminClient.create(properties);

      producer = new KafkaProducer<String, byte[]>(properties);

      LOG.error("NeuUnderFileSystem 构造方法执行完毕");
  }

  @Override
  public String getUnderFSType() {
      LOG.error("getUnderFSType() 执行");
      return "neu";
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
      LOG.error("create()方法执行 path="+path);
    return new NeuFileOutputStream(client,stripPath(path));
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
      LOG.error("deleteDirectory()方法执行 path="+path);
    return true;
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
      LOG.error("deleteFile()方法执行 path="+path);
      String underPath = stripPath(path);
      if(isFile(underPath)){
          try {
              client.delete()
                      .guaranteed()      //删除失败，则客户端持续删除，直到节点删除为止
                      .deletingChildrenIfNeeded()   //删除相关子节点
                      .withVersion(-1)    //无视版本，直接删除
                      .forPath(underPath);
          } catch (Exception e) {
              e.printStackTrace();
          }
      }
      return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
      LOG.error("exists()方法执行 path="+path);
    String underPath = stripPath(path);
    // .5.delta.5a88bcdc-c3b4-4ac4-b89e-089fd0648bf7.TID11.tmp
    if(underPath.endsWith(".tmp") && !underPath.contains("TID")){
      return false;
    }else {
        try {
            return null != client.checkExists().forPath(underPath);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
    return false;
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
      LOG.error("getBlockSizeByte()方法执行 path="+path);
    String underPath = stripPath(path);
    if(exists(underPath)){
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath(underPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        return pathInfo.fileInfo.contentLength;

    }
    else {
        return 100;
    }
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
      LOG.error("getDirectoryStatus()方法执行 path="+path);
      String underPath = stripPath(path);
      if(exists(underPath)){
          byte[] output = new byte[0];
          try {
              output = client.getData().forPath(underPath);
          } catch (Exception e) {
              e.printStackTrace();
          }
          PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
          return new UfsDirectoryStatus(pathInfo.name,pathInfo.owner,
                  pathInfo.group,pathInfo.mode,1564562881806L);
      }else {
          return null;
      }
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
      LOG.error("getFileLocations()方法执行 path="+path);
    List<String> ret = new ArrayList<>();
    ret.add(NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC, mUfsConf));
    return ret;
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return getFileLocations(path);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
      LOG.error("getFileStatus()方法执行 path="+path);
    String underPath = stripPath(path);
    if(isFile(underPath)){
        byte[] output = new byte[0];
        try {
            output = client.getData().forPath(underPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        return new UfsFileStatus(pathInfo.name,pathInfo.fileInfo.contentHash,
                pathInfo.fileInfo.contentLength,pathInfo.fileInfo.lastModified,
                pathInfo.owner,pathInfo.group,pathInfo.mode);
    }else {
        return null;
    }
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
      LOG.error("getSpace()方法执行 path="+path);
    if(type.getValue()==0){
      return 249849593856L;
    }else if (type.getValue()==2){
      return 105187893248L;
    }else {
      return 100000000000L;
    }
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
      LOG.error("getStatus()方法执行 path="+path);
      String underPath = stripPath(path);
      if(exists(underPath)){
          byte[] output = new byte[0];
          try {
              output = client.getData().forPath(underPath);
          } catch (Exception e) {
              e.printStackTrace();
          }
          PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);

          if(isFile(underPath)){
              return new UfsFileStatus(pathInfo.name,pathInfo.fileInfo.contentHash,
                      pathInfo.fileInfo.contentLength,pathInfo.fileInfo.lastModified,
                      pathInfo.owner,pathInfo.group,pathInfo.mode);
          }else {
              return new UfsDirectoryStatus(pathInfo.name,pathInfo.owner,
                      pathInfo.group,pathInfo.mode,1564562881806L);
          }
      }else {
          return null;
      }


  }

  @Override
  public boolean isDirectory(String path) throws IOException {
      LOG.error("isDirectory()方法执行 path="+path);
    String underPath = stripPath(path);
    if(exists(path)){
      return !isFile(path);
    }else {
      return false;
    }
  }

  @Override
  public boolean isFile(String path) throws IOException {
      LOG.error("isFile()方法执行 path="+path);
    String underPath = stripPath(path);
    if(underPath.contains(".alluxio_ufs_blocks")){
      return false;
    }else if (exists(path)){
      // 读元信息
      LOG.error("断点");
      byte[] output = new byte[0];
      try {
        output = client.getData().forPath(underPath);
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error(e.getMessage());
      }
      PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);
        LOG.error("断点2");
        LOG.error(pathInfo.toString());

      return !pathInfo.isDirectory;
    }else {
      return false;
    }
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
      LOG.error("listStatus()方法执行 path="+path);
    String underPath = stripPath(path);
    // 根据zk 获取子节点 getchildlen
    List<String> children = null;
    try {
       children = client.getChildren().forPath(underPath);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if(children != null && children.size() != 0){
      UfsStatus[] rtn = new UfsStatus[children.size()];
      int i = 0;
      for(String child:children){
        String childPath = underPath+"/"+child;
        UfsStatus retStatus;
        // 取元信息出来
        byte[] output = new byte[0];
        try {
          output = client.getData().forPath(childPath);
        } catch (Exception e) {
          e.printStackTrace();
        }
        PathInfo pathInfo = (PathInfo) SerializationUtils.deserialize(output);

        if(isFile(childPath)){
          retStatus = new UfsFileStatus(pathInfo.name,pathInfo.fileInfo.contentHash,
                  pathInfo.fileInfo.contentLength,pathInfo.fileInfo.lastModified,
                  pathInfo.owner,pathInfo.group,pathInfo.mode);
        }else {
          //todo 增加pathInfo的modifiedTime
          retStatus = new UfsDirectoryStatus(pathInfo.name,pathInfo.owner,pathInfo.group,
                  pathInfo.mode,1564562881806L);
        }
        rtn[i++] = retStatus;
      }
      return rtn;
    }else {
      return  null;
    }

  }

  /**
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/offsets
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/commits
   */
  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
      LOG.error("mkdirs()方法执行 path="+path);
    // 传入的一定是目录的路径
    String underPath = stripPath(path);
    if(exists(underPath)){
      return false;
    }else {
      // save to zookeeper
      PathInfo pathInfo = new PathInfo();
      pathInfo.name = underPath;
      pathInfo.isDirectory = true;
      byte[] input = SerializationUtils.serialize(pathInfo);
      try {
        client.create()
                .creatingParentContainersIfNeeded()
                .forPath(underPath, input);
      } catch (Exception e) {
        e.printStackTrace();
      }

      // create topic and partition
      initTopicPartition(underPath);
      return true;
    }

  }

  /**
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/offsets
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/sources/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/state/0/0
   * /Users/hcb/Documents/testFile/dummy3/checkpoint_streaming1/commits
   */
  private void initTopicPartition(String underPath) {
    //将mount的路径取消掉
    String realPath = underPath.replace(rootPath+"/","");

    // 什么都不做 checkpoint_streaming1/state
    if(realPath.endsWith("/state")){
      return;
    }

    // 创建topic
    // checkpoint_streaming1  checkpoint_streaming1/offsets  checkpoint_streaming1/commits
    // checkpoint_streaming1/state/0  checkpoint_streaming1/sources
    else if(realPath.endsWith("/offsets")||realPath.endsWith("/commits")||
            realPath.endsWith("/sources")||!realPath.contains("/")||
            realPath.matches(".*(/state/(\\d){1,5})$")){
            // 创建topic
            String topicName = underPath.replace("/","_").substring(1,underPath.length());
            NewTopic newTopic = new NewTopic(topicName, 1, (short)1);
            List<NewTopic> newTopics = new ArrayList<NewTopic>();
            newTopics.add(newTopic);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopics);
            // 确保topic创建成功
            KafkaFuture kafkaFuture = createTopicsResult.all();
            try {
              kafkaFuture.get();
            } catch (InterruptedException e) {
              e.printStackTrace();
            } catch (ExecutionException e) {
              e.printStackTrace();
            }

    }

    // 增加partition
    // checkpoint_streaming1/sources/0  checkpoint_streaming1/state/0/0
    else if(realPath.matches(".*(/sources/(\\d){1,5})$")||
            realPath.matches(".*(/state/(\\d){1,5})/(\\d){1,5}$")){
            String topicDir = underPath.substring(0,underPath.lastIndexOf("/"));
            String topicName = topicDir.replace("/","_").substring(1,topicDir.length());
            int oldPartitions = producer.partitionsFor(topicName).size();
            int partitionNo = Integer.parseInt(realPath.substring(realPath.lastIndexOf("/")+1,realPath.length()));
            if(partitionNo < oldPartitions){
              // no-op
            }else {
              // 增加到partitionNO+1,因为partitionNO是从0开始
              Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
              newPartitionsMap.put(topicName,NewPartitions.increaseTo(partitionNo+1));
              CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(newPartitionsMap);
              // 确保partition增加成功
              KafkaFuture kafkaFuture = createPartitionsResult.all();
              try {
                kafkaFuture.get();
              } catch (InterruptedException e) {
                e.printStackTrace();
              } catch (ExecutionException e) {
                e.printStackTrace();
              }
        }
    }
    else {
      //todo 增加其他的文件夹判断?
      return;
    }
  }




  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
      LOG.error("open()方法执行 path="+path);
    return new NeuFileInputStream(client,stripPath(path));
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
      LOG.error("renameDirectory()方法执行 src="+src+" dst"+dst);
    return true;
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
      LOG.error("renameFile()方法执行 src="+src+" dst"+dst);
    return true;
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {

  }

  @Override
  public void setMode(String path, short mode) throws IOException {

  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
  }

  @Override
  public boolean supportsFlush() throws IOException {
    return false;
  }

  @Override
  public void cleanup() {}

  /**
   * Sleep and strip scheme from path.
   *
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
//    LOG.debug("Sleeping for configured interval");
//    SleepUtils.sleepMs(mUfsConf.getMs(NeuUnderFileSystemPropertyKey.NEU_UFS_SLEEP));

    if (path.startsWith(NEU_SCHEME)) {
      path = path.substring(NEU_SCHEME.length());
    }
    return new AlluxioURI(path).getPath();
  }

  private Properties getLog4jProps(){
      Properties props = new Properties();
      props.put("log4j.appender.FileAppender","org.apache.log4j.RollingFileAppender");
      props.put("log4j.appender.FileAppender.File","/Users/hcb/Documents/logs2/log4j/neu.log");
      props.put("log4j.appender.FileAppender.layout","org.apache.log4j.PatternLayout");
      props.put("log4j.appender.FileAppender.layout.ConversionPattern","%-4r [%t] %-5p %c %x - %m%n");
      props.put("log4j.rootLogger","ERROR, FileAppender");
      return props;
  }


}
