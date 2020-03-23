/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.CopyCommands;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.namenode.syncservice.MountManager;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceSatisfier;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SyncMountSnapshotUpdateTrackerFactory.TrackerClasses;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.multipart.phases.MultipartPhase;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SyncMountSnapshotUpdateTrackerFactory.TrackerClasses.ALL_MULTIPART;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestGrandSyncTask {
  private final Configuration clientConf = new HdfsConfiguration();
  private final Configuration backupConf = new HdfsConfiguration();
  private MiniDFSCluster backupCluster = null;
  private MiniDFSCluster clientCluster = null;
  private File tempDir;
  private static final Logger LOG = LoggerFactory
      .getLogger(TestGrandSyncTask.class);

  @Before
  public void setup () {
    this.tempDir = Files.createTempDir();
    String baseDirBackup = GenericTestUtils.getTestDir("dfsBackup").getAbsolutePath()
        + File.separator;
    backupConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDirBackup);
    String baseDirClient = GenericTestUtils.getTestDir("dfsClient").getAbsolutePath()
        + File.separator;
    clientConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDirClient);
    clientConf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY,
        true);
  }

  @Test
  public void testCreateMultipartFiles() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test4";
    String fileExtension = ".txt";
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(), ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    URI uri = backupCluster.getURI();
    String syncMountName = "testCreateFiles";
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    int numberOfFiles = 5; //500;

    List<String> allFileContents = new ArrayList<String>(numberOfFiles);
    List<Path> allFilePaths = new ArrayList<Path>(numberOfFiles);

    for(int i = 0; i < numberOfFiles; i++) {
      Path filePath = new Path(backUpPath, fileName + i + "." + fileExtension);
      allFilePaths.add(filePath);
      DFSTestUtil.createFile(clientCluster.getFileSystem(),
          filePath,
          fileLength,
          replFactor,
          0);
      DFSTestUtil.waitForReplication(clientCluster.getFileSystem(), filePath,
          replFactor, 1000);
      String fileContents = DFSTestUtil.readFile(clientCluster.getFileSystem(), filePath);

      assertThat(fileContents).hasSize(fileLength);
      allFileContents.add(fileContents);
    }

    DataNode dataNode = clientCluster.getDataNodes().get(0);

    DataNodeTestUtils.triggerHeartbeat(dataNode);

    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    waitForSyncTasks(clientCluster, syncMount, numberOfFiles);

    for(int j = 0; j < allFilePaths.size(); j++) {
      Path p = allFilePaths.get(j);
      String fileContents = allFileContents.get(j);
      String fName = p.getName();
      Path expectedFilePath = new Path("/", fName);
      waitForFileToExist(backupCluster, expectedFilePath);
      FileStatus fileStatus = backupCluster.getFileSystem().getFileStatus(expectedFilePath);
      assertThat(fileStatus.isFile()).isTrue();

      FSDataInputStream actualFileStream = backupCluster.getFileSystem().open(expectedFilePath);
      byte[] buf = new byte[fileLength];
      actualFileStream.readFully(0, buf, 0, fileLength);
      assertThat(new String(buf)).isEqualTo(fileContents);
    }


    MountManager mountManager =
        clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
        .getNumberOfBytesTransported(syncMount))
        .isEqualTo(fileLength * numberOfFiles);

    assertThat(mountManager
        .getNumberOfSuccessfulBlockOps(syncMount))
        .isEqualTo(numberOfFiles);


  }

  @Test
  public void testCreateFiles() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test4";
    String fileExtension = ".txt";
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    URI uri = backupCluster.getURI();
    String syncMountName = "testCreateFiles";
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    int numberOfFiles = 5; //500;

    List<String> allFileContents = new ArrayList<String>(numberOfFiles);
    List<Path> allFilePaths = new ArrayList<Path>(numberOfFiles);

    for(int i = 0; i < numberOfFiles; i++) {
      Path filePath = new Path(backUpPath, fileName + i + "." + fileExtension);
      allFilePaths.add(filePath);
      DFSTestUtil.createFile(clientCluster.getFileSystem(),
          filePath,
          fileLength,
          replFactor,
          0);
      String fileContents = DFSTestUtil.readFile(clientCluster.getFileSystem(), filePath);
      allFileContents.add(fileContents);
      DFSTestUtil.waitForReplication(clientCluster.getFileSystem(), filePath,
          replFactor, 1000);
    }

    DataNode dataNode = clientCluster.getDataNodes().get(0);

    DataNodeTestUtils.triggerHeartbeat(dataNode);

    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    waitForSyncTasks(clientCluster, syncMount, numberOfFiles);

    for(int j = 0; j < allFilePaths.size(); j++) {
      Path p = allFilePaths.get(j);
      String fileContents = allFileContents.get(j);
      String fName = p.getName();
      Path expectedFilePath = new Path("/", fName);
      waitForFileToExist(backupCluster, expectedFilePath);
      FileStatus fileStatus = backupCluster.getFileSystem().getFileStatus(expectedFilePath);
      assertThat(fileStatus.isFile()).isTrue();

      FSDataInputStream actualFileStream = backupCluster.getFileSystem().open(expectedFilePath);
      byte[] buf = new byte[fileLength];
      actualFileStream.readFully(0, buf, 0, fileLength);
      assertThat(new String(buf)).isEqualTo(fileContents);
    }


    MountManager mountManager =
        clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
        .getNumberOfBytesTransported(syncMount))
        .isEqualTo(fileLength * numberOfFiles);

    assertThat(mountManager
        .getNumberOfSuccessfulBlockOps(syncMount))
        .isEqualTo(numberOfFiles);


  }

  @Test
  public void testCreateFile() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test4.txt";
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    URI uri = backupCluster.getURI();
    String syncMountName = "testCreateFile";
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());

    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    Path filePath = new Path(backUpPath, fileName);

    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        filePath,
        fileLength,
        replFactor,
        0);

    String fileContents = DFSTestUtil.readFile(clientCluster.getFileSystem(), filePath);

    DFSTestUtil.waitForReplication(clientCluster.getFileSystem(), filePath,
        replFactor, 1000);

    DataNode dataNode = clientCluster.getDataNodes().get(0);

    DataNodeTestUtils.triggerHeartbeat(dataNode);

    waitForSyncTasks(clientCluster, syncMount, 1);

    Path expectedFilePath = new Path("/", fileName);
    waitForFileToExist(backupCluster, expectedFilePath);
    FileStatus fileStatus = backupCluster.getFileSystem().getFileStatus(expectedFilePath);
    assertThat(fileStatus.isFile()).isTrue();
    assertThat(fileStatus.getLen()).isEqualTo(fileLength);

    FSDataInputStream actualFileStream = backupCluster.getFileSystem().open(expectedFilePath);
    byte[] buf = new byte[fileLength];
    actualFileStream.readFully(0, buf, 0, fileLength);

    assertThat(new String(buf)).isEqualTo(fileContents);

    MountManager mountManager =
        clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
        .getNumberOfBytesTransported(syncMount))
        .isEqualTo(fileLength);

  }


  @Test
  public void testOverReplication() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test4.txt";
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
            ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
            FsPermission.getDefault());

    URI uri = backupCluster.getURI();
    String syncMountName = "testCreateFile";
    clientCluster
            .getNameNode()
            .getNamesystem()
            .createBackup(syncMountName,
                    backUpPath,
                    uri.toString());

    SyncMount syncMount =
            clientCluster
                    .getNamesystem()
                    .getMountManager()
                    .getSyncMount(syncMountName);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
            HdfsConstants.PROVIDED_STORAGE_ONLY_POLICY_NAME);

    Path filePath = new Path(backUpPath, fileName);

    DFSTestUtil.createFile(clientCluster.getFileSystem(),
            filePath,
            fileLength,
            replFactor,
            0);

    String fileContents = DFSTestUtil.readFile(clientCluster.getFileSystem(), filePath);

    DFSTestUtil.waitForReplication(clientCluster.getFileSystem(), filePath,
            replFactor, 1000);

    DataNode dataNode = clientCluster.getDataNodes().get(0);

    DataNodeTestUtils.triggerHeartbeat(dataNode);

    waitForSyncTasks(clientCluster, syncMount, 1);

    Path expectedFilePath = new Path("/", fileName);
    waitForFileToExist(backupCluster, expectedFilePath);
    FileStatus fileStatus = backupCluster.getFileSystem().getFileStatus(expectedFilePath);
    assertThat(fileStatus.isFile()).isTrue();
    assertThat(fileStatus.getLen()).isEqualTo(fileLength);

    FSDataInputStream actualFileStream = backupCluster.getFileSystem().open(expectedFilePath);
    byte[] buf = new byte[fileLength];
    actualFileStream.readFully(0, buf, 0, fileLength);

    assertThat(new String(buf)).isEqualTo(fileContents);

    MountManager mountManager =
            clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
            .getNumberOfBytesTransported(syncMount))
            .isEqualTo(fileLength);

    Thread.sleep(30000);
    DFSClient dfsClient = new DFSClient(
        DFSUtilClient.getNNAddress(clientConf), clientConf);

    waitForStorageTypesToContain(dfsClient, filePath, StorageType.PROVIDED, 0, 7);

  }


  @Test
  public void testCreateMultipartFile() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test4.txt";
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(), ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    URI uri = backupCluster.getURI();
    String syncMountName = "testCreateFile";
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());

    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    Path filePath = new Path(backUpPath, fileName);

    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        filePath,
        fileLength,
        replFactor,
        0);

    String fileContents = DFSTestUtil.readFile(clientCluster.getFileSystem(), filePath);

    DFSTestUtil.waitForReplication(clientCluster.getFileSystem(), filePath,
        replFactor, 1000);

    DataNode dataNode = clientCluster.getDataNodes().get(0);

    DataNodeTestUtils.triggerHeartbeat(dataNode);

    waitForSyncTasks(clientCluster, syncMount, 1);

    Path expectedFilePath = new Path("/", fileName);
    waitForFileToExist(backupCluster, expectedFilePath);
    FileStatus fileStatus = backupCluster.getFileSystem().getFileStatus(expectedFilePath);
    assertThat(fileStatus.isFile()).isTrue();
    assertThat(fileStatus.getLen()).isEqualTo(fileLength);

    FSDataInputStream actualFileStream = backupCluster.getFileSystem().open(expectedFilePath);
    byte[] buf = new byte[fileLength];
    actualFileStream.readFully(0, buf, 0, fileLength);

    assertThat(new String(buf)).isEqualTo(fileContents);

    MountManager mountManager =
        clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
        .getNumberOfBytesTransported(syncMount))
        .isEqualTo(fileLength);

  }

  @Test
  public void testCreateFileToTmpLocationAndRenameIntoPosition()
      throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test4.txt._COPYING_";
    String renamedFileName = "test4.txt";
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    String syncMountName = "testCreateFileToTmpLocationAndRenameIntoPosition";

    URI uri = backupCluster.getURI();
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());


    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    Path filePath = new Path(backUpPath, fileName);
    Path renamedFilePath = new Path(backUpPath, renamedFileName);

    DFSTestUtil.createFile(clientCluster.getFileSystem(), filePath, fileLength,
        replFactor, 0);

    String fileContents = DFSTestUtil.readFile(clientCluster.getFileSystem(),
        filePath);

    DFSTestUtil.waitForReplication(clientCluster.getFileSystem(), filePath,
        replFactor, 1000);

    DFSTestUtil.renameFile(clientCluster.getFileSystem(), filePath,
        renamedFilePath);

    DataNode dataNode = clientCluster.getDataNodes().get(0);

    DataNodeTestUtils.triggerHeartbeat(dataNode);

    waitForSyncTasks(clientCluster, syncMount, 1);

    Path expectedFilePath = new Path ("/", renamedFileName);
    waitForFileToExist(backupCluster, expectedFilePath);

    FileStatus fileStatus = backupCluster.getFileSystem()
        .getFileStatus(expectedFilePath);
    assertThat(fileStatus.isFile()).isTrue();
    assertThat(fileStatus.getLen()).isEqualTo(fileLength);

    FSDataInputStream actualFileStream = backupCluster.getFileSystem()
        .open(expectedFilePath);
    byte[] buf = new byte[fileLength];
    actualFileStream.readFully(0, buf, 0, fileLength);

    assertThat(new String(buf)).isEqualTo(fileContents);
  }

  @Test
  public void testCreateEmptyFile() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test4.txt";
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    String syncMountName = "testCreateEmptyFile";

    URI uri = backupCluster.getURI();
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());

    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    Path filePath = new Path(backUpPath, fileName);

    clientCluster.getFileSystem().create(filePath).close();

    DFSTestUtil.waitForReplication(clientCluster.getFileSystem(), filePath,
        replFactor, 1000);

    Path expectedFilePath = new Path("/", fileName);
    waitForFileToExist(backupCluster, expectedFilePath);
    FileStatus fileStatus = backupCluster.getFileSystem().getFileStatus(expectedFilePath);
    assertThat(fileStatus.isFile()).isTrue();
    assertEquals(0, fileStatus.getLen());

    MountManager mountManager =
        clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
        .getNumberOfSuccessfulMetaOps(syncMount, MetadataSyncTaskOperation.CREATE_DIRECTORY))
      .isEqualTo(1);

    waitForNumberOfMetaSynctask(mountManager,
        syncMount,
        MetadataSyncTaskOperation.TOUCH_FILE,
        1);
  }

  @Test
  public void testDeleteFile() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test1.txt";
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    String syncMountName = "testDeleteFile";

    URI uri = backupCluster.getURI();
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());

    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    Path toBeDeletedOnClientCluster = new Path(backUpPath, fileName);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        toBeDeletedOnClientCluster,
        fileLength,
        replFactor,
        0);

    waitForFileToExist(backupCluster, new Path("/", fileName));

    //when:

    DFSTestUtil.deleteFile(clientCluster.getFileSystem(), toBeDeletedOnClientCluster);

    //then:

    waitForFileToNotExist(backupCluster, new Path("/", fileName));

    assertThatExceptionOfType(FileNotFoundException.class)
        .isThrownBy( () -> backupCluster
            .getFileSystem()
            .getFileStatus(new Path("/", fileName)))
        .withMessage("File does not exist: " + "/" + fileName);

    MountManager mountManager =
        clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
        .getNumberOfSuccessfulMetaOps(syncMount, MetadataSyncTaskOperation.DELETE_FILE))
        .isEqualTo(1);

  }

  @Test
  public void testRenameFile() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    String fileName = "test2.txt";
    String renamedFileName = "test3.txt";
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    URI uri = backupCluster.getURI();

    String syncMountName = "testRenameFile";

    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());

    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    Path filePath = new Path(backUpPath, fileName);
    Path renamedFilePath = new Path(backUpPath, renamedFileName);

    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        filePath,
        fileLength,
        replFactor,
        0);
    String fileContents = DFSTestUtil.readFile(clientCluster.getFileSystem(), filePath);
    waitForFileToExist(backupCluster, new Path("/", fileName));

    //when:
    DFSTestUtil.renameFile(clientCluster.getFileSystem(), filePath, renamedFilePath);

    //then:
    waitForFileToNotExist(backupCluster, new Path("/", fileName));
    waitForFileToExist(backupCluster, new Path("/", renamedFileName));


    Path expectedFilePath = new Path("/", renamedFileName);
    FileStatus fileStatus = backupCluster.getFileSystem().getFileStatus(expectedFilePath);
    assertThat(fileStatus.isFile()).isTrue();
    assertThat(fileStatus.getLen()).isEqualTo(fileLength);

    FSDataInputStream actualFileStream = backupCluster.getFileSystem().open(expectedFilePath);

    byte[] buf = new byte[fileLength];
    actualFileStream.readFully(0, buf, 0, fileLength);

    assertThat(new String(buf)).isEqualTo(fileContents);

    MountManager mountManager =
        clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
        .getNumberOfSuccessfulMetaOps(syncMount, MetadataSyncTaskOperation.RENAME_FILE))
        .isEqualTo(2);

  }

  @Test
  public void testAppend() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "a"),
        FsPermission.getDefault());


    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "a/f1-to-append.bin"), 1024, replFactor, 0);


    URI uri = backupCluster.getURI();
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup("testAllOperations",
            backUpPath,
            uri.toString());

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    waitForFileToExist(backupCluster, new Path("/a/f1-to-append.bin"));

    //when:

    DFSTestUtil.appendFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "a/f1-to-append.bin"), 2048);

    //then:
    waitForFileToHaveLen(backupCluster, new Path("/a/f1-to-append.bin"), 3072);
  }

  @Test
  public void testDeleteDirectory() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    int fileLength = 7;
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdir(new Path(backUpPath),
        FsPermission.getDefault());

    String syncMountName = "testDeleteDirectory";

    URI uri = backupCluster.getURI();
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());

    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    Path toBeDeletedOnClientCluster = new Path(backUpPath, "test-files");
    clientCluster.getFileSystem().mkdir(toBeDeletedOnClientCluster,
        FsPermission.getDirDefault());
    waitForFileToExist(backupCluster, new Path("/test-files"));

    final int numFiles = 5;
    final String fileNames[] = new String[numFiles];
    for (int i = 0; i < numFiles; ++i) {
      fileNames[i] = "fileName" + i;
      Path filePath = new Path(toBeDeletedOnClientCluster, fileNames[i]);
      DFSTestUtil.createFile(clientCluster.getFileSystem(), filePath, fileLength,
          replFactor, 0);
    }

    for(String fileName : fileNames) {
      waitForFileToExist(backupCluster, new Path("/test-files", fileName));
    }

    //when:
    clientCluster.getFileSystem().delete(toBeDeletedOnClientCluster, true);

    //then:

    waitForFileToNotExist(backupCluster, new Path("/test-files"));

    assertThatExceptionOfType(FileNotFoundException.class)
        .isThrownBy( () -> backupCluster
            .getFileSystem()
            .getFileStatus(new Path("/test-files")))
        .withMessage("File does not exist: " + "/test-files");

    MountManager mountManager =
        clientCluster.getNamesystem().getMountManager();

    assertThat(mountManager
        .getNumberOfSuccessfulMetaOps(syncMount, MetadataSyncTaskOperation.DELETE_DIRECTORY))
        .isEqualTo(1);
    assertThat(mountManager
        .getNumberOfSuccessfulMetaOps(syncMount, MetadataSyncTaskOperation.DELETE_FILE))
        .isEqualTo(numFiles);
  }

  @Test
  public void testAllOperations() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "a"),
        FsPermission.getDefault());
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "b"),
        FsPermission.getDefault());
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "c"),
        FsPermission.getDefault());
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "d"),
        FsPermission.getDefault());


    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "a/f1-to-append.bin"), 1024,replFactor, 0);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "b/f2.bin"), 1024,replFactor, 1);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "c/f3.bin"), 1024,replFactor, 1);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "d/f4.bin"), 1024,replFactor, 1);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "f5.bin"), 1024,replFactor, 1);


    URI uri = backupCluster.getURI();
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup("testAllOperations",
            backUpPath,
            uri.toString());

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    waitForFileToExist(backupCluster, new Path("/a/f1-to-append.bin"));

    //when:

    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "create-dir"),
        FsPermission.getDefault());
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "create-dir/nf1.bin"), 1024, replFactor, 1);

    DFSTestUtil.appendFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "a/f1-to-append.bin"), 2048);

    clientCluster.getFileSystem().rename(new Path(backUpPath, "b"), new Path(backUpPath, "b-renamed"));

    clientCluster.getFileSystem().rename(new Path(backUpPath, "c/f3.bin"), new Path(backUpPath, "f3-newname.bin"));

    clientCluster.getFileSystem().delete(new Path(backUpPath, "d"), true);
    clientCluster.getFileSystem().delete(new Path(backUpPath, "f5.bin"), false);

    //then:
    waitForFileToExist(backupCluster, new Path("/create-dir/nf1.bin"));
    waitForFileToNotExist(backupCluster, new Path("/b/f2.bin"));
    waitForFileToExist(backupCluster, new Path("/b-renamed/f2.bin"));
    waitForFileToNotExist(backupCluster, new Path("/c/f3.bin"));
    waitForFileToExist(backupCluster, new Path("/f3-newname.bin"));
    waitForFileToNotExist(backupCluster, new Path("/d"));
    waitForFileToNotExist(backupCluster, new Path("/d/f4.bin"));
    waitForFileToNotExist(backupCluster, new Path("/f5.bin"));
    waitForFileToHaveLen(backupCluster, new Path("/a/f1-to-append.bin"), 3072);

  }

  @Test
  public void testFileFilter () throws IOException, TimeoutException, InterruptedException {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    SyncServiceSatisfier syncServiceSatisfier =
        clientCluster.getNamesystem().getBlockManager().getSyncServiceSatisfier();
    syncServiceSatisfier.manualMode();
    String filteredFileName = "file._COPYING_";
    String realFileName = "file.txt";
    String syncMountName = "testFileFilter";
    URI uri = backupCluster.getURI();

    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, filteredFileName), 1024, replFactor, 0);

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup(syncMountName,
            backUpPath,
            uri.toString());
    SyncMount syncMount =
        clientCluster
            .getNamesystem()
            .getMountManager()
            .getSyncMount(syncMountName);

    scheduleFullPlan(syncServiceSatisfier);

    waitForSyncServiceSatisfierToFinish(syncServiceSatisfier);

    clientCluster.getFileSystem().rename(
        new Path(backUpPath, filteredFileName),
        new Path(backUpPath, realFileName)
    );

    scheduleFullPlan(syncServiceSatisfier);

    waitForFileToExist(backupCluster, new Path("/" + realFileName));
    waitForSyncServiceSatisfierToFinish(syncServiceSatisfier);
    waitForSyncTasks(clientCluster, syncMount, 1);
    waitForSyncTasks(clientCluster, syncMount, MetadataSyncTaskOperation.RENAME_FILE, 0);

  }

  private void scheduleFullPlan(SyncServiceSatisfier syncServiceSatisfier) throws InterruptedException, IOException {
    final int totalNumPhases = PhasedPlan.Phases.values().length +
        MultipartPhase.values().length;
    for (int i = 0; i < totalNumPhases; ++i) {
      syncServiceSatisfier.scheduleOnce();
      //This is bad, should wait for the phase to complete
      Thread.sleep(100);
    }
  }


  @Test
  public void testCircularRename() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "a"),
        FsPermission.getDefault());
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "b"),
        FsPermission.getDefault());
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "c"),
        FsPermission.getDefault());

    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "a/f1.bin"), 1024,replFactor, 0);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "b/f2.bin"), 1024,replFactor, 1);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "c/f3.bin"), 1024,replFactor, 1);


    URI uri = backupCluster.getURI();
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup("testCircularRename",
            backUpPath,
            uri.toString());

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    waitForFileToExist(backupCluster, new Path("/a/f1.bin"));
    waitForFileToExist(backupCluster, new Path("/b/f2.bin"));
    waitForFileToExist(backupCluster, new Path("/c/f3.bin"));

    //when:

    clientCluster.getFileSystem().rename(new Path(backUpPath, "c"), new Path(backUpPath, "z"));
    clientCluster.getFileSystem().rename(new Path(backUpPath, "b"), new Path(backUpPath, "c"));
    clientCluster.getFileSystem().rename(new Path(backUpPath, "a"), new Path(backUpPath, "b"));
    clientCluster.getFileSystem().rename(new Path(backUpPath, "z"), new Path(backUpPath, "a"));

    //then:
    waitForFileToExist(backupCluster, new Path("/a/f3.bin"));
    waitForFileToExist(backupCluster, new Path("/b/f1.bin"));
    waitForFileToExist(backupCluster, new Path("/c/f2.bin"));

  }

  @Test
  public void testRenameWithReusedNames() throws Exception {
    backupCluster = startClusterWithoutAliasmap();
    int aliasMapPort = NetUtils.getFreeSocketPort();
    short replFactor = 1;
    String backUpPath = "/backMeUp";

    clientCluster = startCluster(aliasMapPort, tempDir.getAbsolutePath(),
        ALL_MULTIPART);
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "a-source"),
        FsPermission.getDefault());
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "a-dest"),
        FsPermission.getDefault());

    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "a-source/f1.bin"), 1024,replFactor, 0);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "a-dest/f2.bin"), 1024,replFactor, 1);


    URI uri = backupCluster.getURI();
    clientCluster
        .getNameNode()
        .getNamesystem()
        .createBackup("testRenameWithReusedNames",
            backUpPath,
            uri.toString());

    clientCluster.getFileSystem().setStoragePolicy(new Path(backUpPath),
        HdfsConstants.DISK_PROVIDED_STORAGE_POLICY_NAME);

    waitForFileToExist(backupCluster, new Path("/a-source/f1.bin"));
    waitForFileToExist(backupCluster, new Path("/a-dest/f2.bin"));

    //when:
    clientCluster.getFileSystem().delete(new Path(backUpPath, "a-dest"), true);
    clientCluster.getFileSystem().rename(new Path(backUpPath, "a-source"), new Path(backUpPath, "a-dest"));
    clientCluster.getFileSystem().mkdirs(new Path(backUpPath, "a-source"),
        FsPermission.getDefault());
    Thread.sleep(30000);
    DFSTestUtil.createFile(clientCluster.getFileSystem(),
        new Path(backUpPath, "a-source/f3.bin"), 1024,replFactor, 0);

    //then:
    waitForFileToNotExist(backupCluster, new Path("/a-source/f1.bin"));
    waitForFileToNotExist(backupCluster, new Path("/a-dest/f2.bin"));
    waitForFileToExist(backupCluster, new Path("/a-dest/f1.bin"));
    waitForFileToExist(backupCluster, new Path("/a-source/f3.bin"));

  }

  @After
  public void teardown () throws IOException {
    clientCluster.shutdown(true, true);
    backupCluster.shutdown(true, true);
    FileUtils.deleteDirectory(this.tempDir);
  }

  private void waitForSyncTasks(MiniDFSCluster cluster,
      SyncMount syncMount,
      MetadataSyncTaskOperation operation,
      int expected) throws InterruptedException,  TimeoutException {
    GenericTestUtils.waitFor(() -> {
      long actual = cluster
          .getNamesystem()
          .getMountManager()
          .getNumberOfSuccessfulMetaOps(syncMount, operation);
       return actual >= expected;
    }, 100, 30000);
  }

  private void waitForSyncTasks(MiniDFSCluster cluster, SyncMount syncMount,
      int expected) throws InterruptedException,  TimeoutException {
    GenericTestUtils.waitFor(() -> {
      long actual = cluster
          .getNamesystem()
          .getMountManager()
          .getNumberOfSuccessfulBlockOps(syncMount);
      LOG.info("actual {}, expected {}", actual, expected);
      return actual >= expected;
    }, 100, 60000);
  }

  private void waitForFileToExist(MiniDFSCluster cluster, Path filePath)
      throws InterruptedException,  TimeoutException{
    GenericTestUtils.waitFor(() -> {
      try {
        cluster.getFileSystem().getFileStatus(filePath);
        return true;
      } catch (IOException e) {
        return false;
      }
    }, 100, 30000);
  }


  private void waitForNumberOfMetaSynctask(MountManager mountManager,
      SyncMount syncMount,
      MetadataSyncTaskOperation operation,
      int number)
      throws InterruptedException,  TimeoutException{
    GenericTestUtils.waitFor(() -> mountManager
       .getNumberOfSuccessfulMetaOps(syncMount, operation)
       == number, 100, 30000);
  }


  private void waitForSyncServiceSatisfierToFinish(SyncServiceSatisfier syncServiceSatisfier)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> ! syncServiceSatisfier.hasSyncingInProgress(),
        100, 60000);
  }



  private void waitForFileToNotExist(MiniDFSCluster cluster, Path filePath)
      throws InterruptedException,  TimeoutException{
    GenericTestUtils.waitFor(() -> {
      try {
        cluster.getFileSystem().getFileStatus(filePath);
        return false;
      } catch (FileNotFoundException e) {
        return true;
      } catch (IOException e) {
        return false;
      }
    }, 100, 30000);
  }


  private void waitForStorageTypesToContain(DFSClient dfsClient,
      Path filePath,
      StorageType storageType,
      long start,
      long length) throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> {
      try {
        BlockLocation[] blockLocations = dfsClient.
            getBlockLocations(filePath.toString(), start, length);
        assertThat(blockLocations).hasSize(1);
        BlockLocation blockLocation = blockLocations[0];
        StorageType[] storageTypes = blockLocation.getStorageTypes();
        assertThat(storageTypes).hasSize(1);
        return storageTypes[0] == storageType;
      } catch (IOException e) {
        return false;
      }
    }, 1000, 30000);

  }

  private void waitForFileToHaveLen(MiniDFSCluster cluster, Path filePath, int len)
      throws InterruptedException,  TimeoutException{
    GenericTestUtils.waitFor(() -> {
      try {
        return cluster.getFileSystem().getFileStatus(filePath).getLen() == len;
      } catch (IOException e) {
        return false;
      }
    }, 100, 30000);
  }


  private void assertFilesAndBytesSynced(MiniDFSCluster cluster,
      DatanodeID datanodeID, int filesSynced, long bytesSynced)
      throws UnregisteredNodeException {
    DatanodeDescriptor datanode = cluster
        .getNamesystem()
        .getBlockManager()
        .getDatanodeManager()
        .getDatanode(datanodeID);
    assertEquals(filesSynced,
        datanode.getBackupStatistics().getNumFilesBackedUp());
    assertEquals(bytesSynced,
        datanode.getBackupStatistics().getTotalBytesBackedUp());
  }

  private void writeOutputStream(String fileContents,
      File file,
      MiniDFSCluster cluster) throws IOException {
    Path path = new Path(file.getPath());
    FSDataOutputStream fsDataOutputStream =
        cluster.getFileSystem().create(path);
    fsDataOutputStream.writeBytes(fileContents);
    fsDataOutputStream.close();
  }

  private MiniDFSCluster startCluster(int aliasMapPort,
      String aliasMapDir,
      TrackerClasses trackerType) throws IOException {
    clientConf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
        "org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.InMemoryLevelDBAliasMapClient");
    clientConf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
        "localhost:" + aliasMapPort);
    clientConf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_BIND_HOST,
        "localhost");
    clientConf.setBoolean(DFSConfigKeys.DFS_NAMENODE_PROVIDED_ENABLED, true);
    clientConf.setBoolean(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_ENABLED, true);
    clientConf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR, aliasMapDir);
    clientConf.setEnum(DFSConfigKeys.DFS_SNAPSHOT_UPDATE_TRACKER, trackerType);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(clientConf).numDataNodes(1)
        .storageTypes(
            new StorageType[][]{{StorageType.DISK}})
        .build();
    cluster.waitActive();
    return cluster;
  }

  private MiniDFSCluster startClusterWithoutAliasmap() throws IOException {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(backupConf).numDataNodes(1)
        .storageTypes(
            new StorageType[][]{{StorageType.DISK}})
        .build();
    cluster.waitActive();
    return cluster;
  }
}
