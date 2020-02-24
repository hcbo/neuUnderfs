# kafka容错系统

## 环境

spark 2.4.3

kafka_2.11-2.1.0

zookeeper 3.4.10

alluxio 2.0.0

## 创建alluxio集群

[ Deploy an Alluxio Cluster with a Single Master](https://docs.alluxio.io/os/user/stable/en/deploy/Running-Alluxio-On-a-Cluster.html)



## 部署
- mvn package -Dmaven.test.skip=true
- [向spark中添加alluxio cilent](https://docs.alluxio.io/os/user/stable/en/compute/Spark.html)
- 开启zookeeper
- 开启kafka
- 开启kafka-manager
- 开启zkUI
- alluxio操作
  - ./bin/alluxio format
  - ./bin/alluxio-start.sh all SudoMount
  - ./bin/alluxio extensions install [.jar]
  - ./bin/alluxio fs mount /neu neu:///kafkaFS —option fs.neu.kafkaServers=[kafkaServers] —option fs.neu.zkSevers=[zkServers]

## 使用

示例代码

```scala
val dataStreamWriter = wordcount
      .writeStream
      .queryName("kafka_test2")
      .option("checkpointLocation","alluxio://vmceph:19998/neu/checkpoint_streaming5")
      .outputMode(OutputMode.Complete())
      .format("console")

val query = dataStreamWriter.start()
query.awaitTermination()

```

## 备注

鉴于多次测试,检查点目录内的文件比较小,所以为了节省开销,提高性能,只为每个文件分配了1024字节缓冲.

kafka消息默认保留7天,只有metadata保留了70天.

mount时指定单层路径.

以上都可以通过代码很容易修改.

