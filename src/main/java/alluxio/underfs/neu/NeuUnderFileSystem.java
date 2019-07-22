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
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.underfs.options.*;
import alluxio.util.SleepUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * neu {@link UnderFileSystem} implementation for tutorial. All operations are delegated to a
 * {@link LocalUnderFileSystem}.
 */
@ThreadSafe
public class NeuUnderFileSystem extends ConsistentUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(NeuUnderFileSystem.class);

  public static final String NEU_SCHEME = "neu://";

  private UnderFileSystem mLocalUnderFileSystem;

  /**
   * Constructs a new {@link NeuUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf UFS configuration
   */
  public NeuUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration conf) {
    super(uri, conf);

    mLocalUnderFileSystem =
        new LocalUnderFileSystem(new AlluxioURI(stripPath(uri.getPath())), conf);
  }

  @Override
  public String getUnderFSType() {
    return "neu";
  }

  @Override
  public void close() throws IOException {
    mLocalUnderFileSystem.close();
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return mLocalUnderFileSystem.create(stripPath(path), options);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    return mLocalUnderFileSystem.deleteDirectory(stripPath(path), options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return mLocalUnderFileSystem.deleteFile(stripPath(path));
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mLocalUnderFileSystem.exists(stripPath(path));
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return mLocalUnderFileSystem.getBlockSizeByte(stripPath(path));
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getDirectoryStatus(stripPath(path));
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return mLocalUnderFileSystem.getFileLocations(stripPath(path));
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return mLocalUnderFileSystem.getFileLocations(stripPath(path), options);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getFileStatus(stripPath(path));
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return mLocalUnderFileSystem.getSpace(stripPath(path), type);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getStatus(path);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return mLocalUnderFileSystem.isDirectory(stripPath(path));
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mLocalUnderFileSystem.isFile(stripPath(path));
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    return mLocalUnderFileSystem.listStatus(stripPath(path));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    return mLocalUnderFileSystem.mkdirs(stripPath(path), options);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return mLocalUnderFileSystem.open(stripPath(path), options);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    return mLocalUnderFileSystem.renameDirectory(stripPath(src), stripPath(dst));
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    return mLocalUnderFileSystem.renameFile(stripPath(src), stripPath(dst));
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    mLocalUnderFileSystem.setOwner(stripPath(path), user, group);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    mLocalUnderFileSystem.setMode(stripPath(path), mode);
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    mLocalUnderFileSystem.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    mLocalUnderFileSystem.connectFromWorker(hostname);
  }

  @Override
  public boolean supportsFlush() throws IOException {
    return mLocalUnderFileSystem.supportsFlush();
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
    LOG.debug("Sleeping for configured interval");
    SleepUtils.sleepMs(mUfsConf.getMs(NeuUnderFileSystemPropertyKey.NEU_UFS_SLEEP));

    if (path.startsWith(NEU_SCHEME)) {
      path = path.substring(NEU_SCHEME.length());
    }
    return new AlluxioURI(path).getPath();
  }
}
