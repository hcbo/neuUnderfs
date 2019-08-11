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

import alluxio.conf.PropertyKey;

import javax.annotation.concurrent.ThreadSafe;

/**
 * neu under file system property keys.
 */
@ThreadSafe
public class NeuUnderFileSystemPropertyKey {
  public static final PropertyKey NEU_UFS_SLEEP =
      new PropertyKey.Builder(Name.NEU_UFS_SLEEP)
          .setDescription("Sleep time before performing operation on local ufs.")
          .setDefaultValue("0ms")
          .build();
  public static final PropertyKey KAFKA_SERVERS =
          new PropertyKey.Builder(Name.KAFKA_SERVERS)
                  .setDescription("kafka servers ip address and port.")
                  .setDefaultValue("192.168.225.6:9092")
                  .build();
  public static final PropertyKey ZK_SERVERS =
          new PropertyKey.Builder(Name.ZK_SERVERS)
                  .setDescription("zookeeper servers ip address and port.")
                  .setDefaultValue("192.168.225.6:2181")
                  .build();


  @ThreadSafe
  public static final class Name {
    public static final String NEU_UFS_SLEEP = "fs.neu.sleep";
    public static final String KAFKA_SERVERS = "fs.neu.kafkaServers";
    public static final String ZK_SERVERS = "fs.neu.zkSevers";
  }
}
