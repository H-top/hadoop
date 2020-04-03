/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryLevelDBAliasMapServer;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.InMemoryLevelDBAliasMapClient;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.impl.TextFileRegionAliasMap;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.syncservice.MiniDFSClusterRule;
import org.apache.hadoop.hdfs.server.namenode.syncservice.executor.MetadataSyncOperationExecutor;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedSyncMountSnapshotUpdateFactory;
import org.apache.hadoop.hdfs.server.protocol.MetadataSyncTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.CREATE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.MODIFY;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport.partition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;

public class TestMountManager {

  @Rule
  public MiniDFSClusterRule clusterRule = new MiniDFSClusterRule(false);
  private MiniDFSCluster cluster;
  @Before
  public void setup() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster.setupNamenodeProvidedConfiguration(conf);
    cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(1)
            .build();
    cluster.waitActive();
  }

  @Test
  public void testMountManagerAddBackupMountAndAddFiles() throws Exception {
    MiniDFSCluster cluster = clusterRule.getCluster();
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    fileSystem.mkdirs(new Path("/test1/a/b/c"), FsPermission.getDefault());

    NameNode nameNode = cluster.getNameNode();
    MountManager mountManager = nameNode.getNamesystem().getMountManagerSync();
    SyncMount syncMount = new SyncMount("test-backupName-1",
            new Path("/test1"), URI.create("hdfs://test1"));
    mountManager.createBackup(syncMount);

    List<SyncMount> syncMounts = mountManager.getSyncMounts();
    assertEquals(1, syncMounts.size());

    SyncMount syncMount2 = syncMounts.get(0);
    assertEquals("test-backupName-1", syncMount2.getName());
    assertEquals("/test1", syncMount2.getLocalPath().toString());
    assertEquals("hdfs://test1", syncMount2.getRemoteLocation().toString());

    SnapshotDiffReport diffReportBefore = mountManager.makeSnapshotAndPerformDiff(new Path("/test1"));
    assertEquals(1, diffReportBefore.getDiffList().size()); // +
    assertEquals(CREATE, diffReportBefore.getDiffList().get(0).getType());

    FSDataOutputStream stream = fileSystem.create(new Path("/test1/test1.txt"));
    stream.writeBytes("hello world");
    stream.close();
    SnapshotDiffReport diffReportAfter = mountManager.makeSnapshotAndPerformDiff(new Path("/test1"));
    assertEquals(2, diffReportAfter.getDiffList().size()); // M, +
    System.out.println(Arrays.toString(diffReportAfter.getDiffList().toArray()));
    assertEquals(MODIFY, diffReportAfter.getDiffList().get(0).getType());
    assertEquals(CREATE, diffReportAfter.getDiffList().get(1).getType());
  }

  @Test
  @Ignore("TODO: Snapshots aren't removed yet as the scheduler " +
          "doesn't wait for existing snapshots to be completed before continuing.")
  public void testMountManagerSnapshots() throws Exception {
//    MiniDFSCluster cluster = clusterRule.getCluster();
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    Configuration conf = new Configuration();
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem namesystem = nameNode.getNamesystem();
    BlockManager blockManager = namesystem.getBlockManager();
    Path localBackupPath = new Path("/test");
    Path localBackupPath1 = new Path("/test1");
    fileSystem.mkdirs(localBackupPath, FsPermission.getDefault());
    fileSystem.mkdirs(localBackupPath1, FsPermission.getDefault());
    INode inode_root = namesystem.getFSDirectory().getINode(localBackupPath.toString());
    namesystem.allowSnapshot(localBackupPath.toString());
    namesystem.allowSnapshot(localBackupPath1.toString());
    fileSystem.mkdirs(new Path("/test/sub"));
    fileSystem.mkdirs(new Path("/test1/sub"));
    namesystem.createSnapshot(localBackupPath.toString(), "test-s0", false);
    namesystem.createSnapshot(localBackupPath1.toString(), "test1-s0", false);

    FSDataOutputStream out = null;
    out = fileSystem.createFile(new Path("/test/sub/worker")).build();
    out.write("worker".getBytes());
    out.close();
    fileSystem.rename(new Path("/test/sub"), new Path("/test/sub-r"));

    fileSystem.rename(new Path("/test1/sub"), new Path("/test1/sub-r"));
    out = fileSystem.createFile(new Path("/test1/sub-r/worker")).build();
    out.write("worker".getBytes());
    out.close();

//    INodeDirectory iNodeDirectory = inode_root.asDirectory();
//    DirectoryWithSnapshotFeature.DirectoryDiffList diffs = iNodeDirectory.getDirectorySnapshottableFeature().getDiffs();
//    System.out.println("****************create dir******************");
//    System.out.println(diffs);
//
//    out = fileSystem.createFile(new Path("/test/worker2")).build();
//    out.write("worker2".getBytes());
//    out.close();
//    out = fileSystem.createFile(new Path("/test/worker3")).build();
//    out.write("worker3".getBytes());
//    out.close();
//    diffs = iNodeDirectory.getDirectorySnapshottableFeature().getDiffs();
//    System.out.println("****************create file******************");
//    System.out.println(diffs);
//    out = fileSystem.appendFile(new Path("/test/worker1")).build();
//    out.write("add".getBytes());
//    out.close();
//    fileSystem.delete(new Path("/test/worker2"), true);
//    fileSystem.rename(new Path("/test/worker3"), new Path("/test/worker3-renamed"));
//    fileSystem.rename(new Path("/test/sub1"), new Path("/test/sub11"));
//    out = fileSystem.createFile(new Path("/test/sub11/worker")).build();
//    out.write("worker".getBytes());
//    out.close();
//    diffs = iNodeDirectory.getDirectorySnapshottableFeature().getDiffs();
//    System.out.println("****************M******************");
//    System.out.println(diffs);
//    namesystem.createSnapshot(localBackupPath.toString(), "s1", false);

    namesystem.createSnapshot(localBackupPath.toString(), "test-s1", false);
    namesystem.createSnapshot(localBackupPath1.toString(), "test1-s1", false);
    SnapshotDiffReport diffReport = fileSystem.getSnapshotDiffReport(localBackupPath, "test-s0", "test-s1");
    SnapshotDiffReport diffReport1 = fileSystem.getSnapshotDiffReport(localBackupPath1, "test1-s0", "test1-s1");
    System.out.println("*************************************");
    System.out.println(diffReport);
    System.out.println(diffReport1);
    List<DiffReportEntry> entries = diffReport.getDiffList().stream().filter(entry -> entry.getType().equals(CREATE)).collect(Collectors.toList());
    for (DiffReportEntry entry : entries) {
      String src = new String(entry.getSourcePath());
      File path = new File("/test/.snapshot/test-s0/sub");
      INode node = namesystem.getFSDirectory().getINode(path.getAbsolutePath());
      System.out.println(node.getFullPathName());
    }
//    List<DiffReportEntry> entries = diffReport.getDiffList();
//    System.out.println(entries.size());
//    for (DiffReportEntry entry : entries) {
//      System.out.println(entry);
//    }
    System.out.println("*************************************");
//    String abspath = localBackupPath.toString() + "/.snapshot/" + snapshotName;
//    String absfile = new File(abspath, "sub").getAbsolutePath();
//    System.out.println(absfile);
//    INode iNode = namesystem.getFSDirectory().getINode(absfile);
//    for (INode iNode1 : iNode.asDirectory().getChildrenList(inode_root.asDirectory().getSnapshot(snapshotName.getBytes()).getId())){
//      System.out.println(iNode1.getFullPathName());
//    }
//    System.out.println("child");
//    System.out.println(iNode.getFullPathName());
//
//    SnapshotDiffReport diff = fileSystem.getSnapshotDiffReport(localBackupPath, snapshotName, ".");
//    for (SnapshotDiffReport.DiffReportEntry entry : diff.getDiffList()){
//      System.out.println(entry);
//    }
//
//    List<DiffReportEntry> entryList = Lists.newArrayList();
//    DiffReportEntry entry = new DiffReportEntry(SnapshotDiffReport.INodeType.DIRECTORY, SnapshotDiffReport.DiffType.CREATE,
//            ".".getBytes());
//    entryList.add(entry);
//    SnapshotDiffReport diffReport = new SnapshotDiffReport(
//            localBackupPath.toString(), null, snapshotName, entryList);
//    PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdatePlanFactory
//            = new PhasedSyncMountSnapshotUpdateFactory(namesystem, blockManager, conf);
//    PartitionedDiffReport partitionedDiffReport = partition(diffReport,
//            new DefaultSyncServiceFileFilterImpl());
//    System.out.println("***********diff out************");
//    System.out.println(diffReport.getLaterSnapshotName() + "\t" + diffReport.getFromSnapshot() + "\t" + diffReport.getSnapshotRoot());
//    System.out.println(diffReport);
//    System.out.println(diffReport.getDiffList());
//    System.out.println("***********diff out************");
//    SnapshotDiffReport diff1 =
//    mountManager.makeSnapshotAndPerformDiff(new Path("/test1"));
//
//    DiffReportEntry diffEntry1 = diff1.getDiffList().get(0);
//
//    assertEquals(CREATE, diffEntry1.getType());
//    assertEquals(".", new String(diffEntry1.getSourcePath()));
//
//    fileSystem.mkdirs(new Path("/test1/a/b/d"), FsPermission.getDefault());
//    SnapshotDiffReport diff2 =
//    mountManager.makeSnapshotAndPerformDiff(new Path("/test1"));
//    List<DiffReportEntry> diffList2 = diff2.getDiffList();
//    DiffReportEntry diffEntry2 = diffList2.stream()
//        .filter(d -> CREATE.equals(d.getType()))
//        .findFirst()
//        .get();
//    assertEquals(CREATE, diffEntry2.getType());
//    assertEquals("a/b/d", new String(diffEntry2.getSourcePath()));
//
//    // When we make further snapshots for backup, the leftover snapshots are
//    // removed.
//    assertEquals(1, namesystem.getNumSnapshots());
//    mountManager.makeSnapshotAndPerformDiff(new Path("/test1"));
//    assertEquals(1, namesystem.getNumSnapshots());
  }

  @Test
  public void testSyncOpsReturnExceptionWhenSyncServiceNotEnabled() throws IOException {
    Configuration backupConf = new HdfsConfiguration();
    String baseDirBackup = GenericTestUtils.getTestDir("dfsBackup").getAbsolutePath()
            + File.separator;
    backupConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDirBackup);

    MiniDFSCluster cluster = clusterRule.getCluster();
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    fileSystem.mkdirs(new Path("/test1/a/b/c"), FsPermission.getDefault());
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem namesystem = nameNode.getNamesystem();
    String backupName = "test-backupName-1";

  }

  @Test
  public void testMountManagerSnapshot() throws Exception {
    MiniDFSCluster cluster = clusterRule.getCluster();
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    Path localBackupPath = new Path("/test");
    fileSystem.mkdirs(localBackupPath, FsPermission.getDefault());
    Configuration conf = new Configuration();
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem namesystem = nameNode.getNamesystem();
    BlockManager blockManager = namesystem.getBlockManager();

    INode inode_root = namesystem.getFSDirectory().getINode(localBackupPath.toString());
    MountManager mountManager = namesystem.getMountManagerSync();
    List<SnapshotDiffReport.DiffReportEntry> entries = new ArrayList<>();
    entries.add(new SnapshotDiffReport.DiffReportEntry(SnapshotDiffReport.INodeType.DIRECTORY, CREATE, ".".getBytes()));
    SnapshotDiffReport diffReport = new SnapshotDiffReport(localBackupPath.toString(), null, "s0", entries);
    int targetSnapshotId = 12;
    Optional<Integer> sourceSnapshotId = Optional.empty();
    PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdatePlanFactory
            = new PhasedSyncMountSnapshotUpdateFactory(namesystem, blockManager, conf);
    SyncMount syncMount = new SyncMount("backup", localBackupPath, new URI("s3a://test"));
    PhasedPlan planFromDiffReport = syncMountSnapshotUpdatePlanFactory.
            createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
                    targetSnapshotId);


    fileSystem.mkdirs(new Path("/test/sub/subsub"));
    namesystem.allowSnapshot(localBackupPath.toString());
    String snapshotName = "s0";
    namesystem.createSnapshot(localBackupPath.toString(), snapshotName, false);
    fileSystem.delete(new Path("/test/sub"), true);
  }
  @Test
  //对于空的synctask，NN不会schedule而是直接返回
  public void testEmprySynctask() throws Exception{
    List<MetadataSyncTask> syncTasks = Lists.newArrayList();
    Configuration conf = new Configuration();
    MetadataSyncOperationExecutor metadataSyncOperationExecutor = MetadataSyncOperationExecutor.createOnNameNode(conf);
    for (MetadataSyncTask syncTask : syncTasks) {
      SyncTaskExecutionResult result = metadataSyncOperationExecutor.execute(syncTask);
    }
  }
  private BlockAliasMap.Reader<FileRegion> createAliasMapReader(
          BlockManager blockManager, Configuration conf) {
    // load block writer into storage
    Class<? extends BlockAliasMap> aliasMapClass = conf.getClass(
            DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
            TextFileRegionAliasMap.class, BlockAliasMap.class);
    final BlockAliasMap<FileRegion> aliasMap = ReflectionUtils.newInstance(
            aliasMapClass, conf);
    try {
      return aliasMap.getReader(null, blockManager.getBlockPoolId());
    } catch (IOException e) {
      throw new RuntimeException("Could not load AliasMap Reader: "
              + e.getMessage());
    }
  }
  private BlockAliasMap.Writer<FileRegion> createAliasMapWriter(
          BlockManager blockManager, Configuration conf) {
    // load block writer into storage
    Class<? extends BlockAliasMap> aliasMapClass = conf.getClass(
            DFSConfigKeys.DFS_PROVIDED_ALIASMAP_CLASS,
            TextFileRegionAliasMap.class, BlockAliasMap.class);
    final BlockAliasMap<FileRegion> aliasMap = ReflectionUtils.newInstance(
            aliasMapClass, conf);
    try {
      return aliasMap.getWriter(null, blockManager.getBlockPoolId());
    } catch (IOException e) {
      throw new RuntimeException("Could not load AliasMap Writer: "
              + e.getMessage());
    }
  }
  @Test
  public void testAliasMap() throws Exception {
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    Configuration conf = new Configuration();
    NameNode nameNode = cluster.getNameNode();
    InMemoryLevelDBAliasMapServer aliasMapServer = nameNode.getAliasMapServer();
    FSNamesystem namesystem = nameNode.getNamesystem();
    BlockManager blockManager = namesystem.getBlockManager();
    BlockAliasMap.Reader<FileRegion> aliasMapReader = createAliasMapReader(blockManager, conf);
    BlockAliasMap.Writer<FileRegion> aliasMapWriter = createAliasMapWriter(blockManager, conf);
    Block block = new Block(1);
    byte[] nonce = new byte[0];
    ProvidedStorageLocation providedStorageLocation
            = new ProvidedStorageLocation(new Path("file"), 0, block.getNumBytes(),
            nonce);
    FileRegion fileRegion = new FileRegion(block, providedStorageLocation);
    aliasMapServer.write(block, providedStorageLocation);
    aliasMapWriter.store(fileRegion);
    Optional<FileRegion> read = aliasMapServer.read(block.getBlockId());
    Optional<FileRegion> resolve = aliasMapReader.resolve(block.getBlockId());
    System.out.println("**************print****************");
    System.out.println(resolve);
    System.out.println(read.get().getProvidedStorageLocation().getPath());
  }
  @Test
  public void testGenSyncTask() throws Exception {
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    Configuration conf = fileSystem.getConf();
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem fsNamesystem = nameNode.getNamesystem();
    BlockManager blockManager = fsNamesystem.getBlockManager();
    Path backup = new Path("/backup");
    fileSystem.mkdirs(backup);
    MountManager mountManager = new MountManager(fsNamesystem);
    mountManager.createBackup(backup, new URI("http://test:9001"));
    fileSystem.mkdirs(new Path("/backup/test"));
    fileSystem.create(new Path("/backup/test/file"));
    PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdatePlanFactory
            = new PhasedSyncMountSnapshotUpdateFactory(fsNamesystem, blockManager, conf);
    for (SyncMount syncMount : mountManager.getSyncMounts()) {
      SnapshotDiffReport diffReport = mountManager.makeSnapshotAndPerformDiff(backup);
      Optional<Integer> sourceSnapshotId = getSourceSnapshotId(diffReport, fsNamesystem);
      int targetSnapshotId = getTargetSnapshotId(diffReport, fsNamesystem);
      PhasedPlan planFromDiffReport = syncMountSnapshotUpdatePlanFactory.
              createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
                      targetSnapshotId);

    }
    fileSystem.rename(new Path("/backup/test"), new Path("/backup/test-r"));
    fileSystem.delete(new Path("/backup/test-r/file"), false);
    fileSystem.mkdirs(new Path("/backup/test2"));
    for (SyncMount syncMount : mountManager.getSyncMounts()) {
      SnapshotDiffReport diffReport = mountManager.makeSnapshotAndPerformDiff(backup);
      Optional<Integer> sourceSnapshotId = getSourceSnapshotId(diffReport, fsNamesystem);
      int targetSnapshotId = getTargetSnapshotId(diffReport, fsNamesystem);
      PhasedPlan planFromDiffReport = syncMountSnapshotUpdatePlanFactory.
              createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
                      targetSnapshotId);
      System.out.println(planFromDiffReport);
    }

  }
  private Optional<Integer> getSourceSnapshotId(SnapshotDiffReport diffReport, Namesystem namesystem)
          throws UnresolvedLinkException, AccessControlException,
          ParentNotDirectoryException {
    if (diffReport.getFromSnapshot() == null) {
      return Optional.empty();
    }
    INode localBackupPathINode = namesystem.getFSDirectory()
            .getINode(diffReport.getSnapshotRoot());
    INodeDirectory localBackupPathINodeDirectory =
            localBackupPathINode.asDirectory();
    Snapshot toSnapshot = localBackupPathINodeDirectory.getSnapshot(
            diffReport.getFromSnapshot().getBytes());
    return Optional.of(toSnapshot.getId());
  }

  /**
   * 从diffreport获取target snapshot id
   */
  private int getTargetSnapshotId(SnapshotDiffReport diffReport, Namesystem namesystem)
          throws UnresolvedLinkException, AccessControlException,
          ParentNotDirectoryException {
    INode localBackupPathINode = namesystem.getFSDirectory()
            .getINode(diffReport.getSnapshotRoot());
    INodeDirectory localBackupPathINodeDirectory =
            localBackupPathINode.asDirectory();
    Snapshot toSnapshot = localBackupPathINodeDirectory.getSnapshot(
            diffReport.getLaterSnapshotName().getBytes());
    return toSnapshot.getId();
  }

  @Test
  public void testInmemoryAliasMap() throws Exception{
    Configuration conf = new Configuration();
    int port = NetUtils.getFreeSocketPort();
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
            "localhost:" + port);
    File tempDir = Files.createTempDir();
    String BPID = "BP-1296449652-10.1.0.12-1585139437775";
    new File(tempDir, BPID).mkdirs();
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR,
            tempDir.getAbsolutePath());
    InMemoryLevelDBAliasMapServer levelDBAliasMapServer = new InMemoryLevelDBAliasMapServer(InMemoryAliasMap::init, BPID);
    InMemoryLevelDBAliasMapClient inMemoryLevelDBAliasMapClient = new InMemoryLevelDBAliasMapClient();
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR, "/tmp/aliasmap");
    levelDBAliasMapServer.setConf(conf);
    levelDBAliasMapServer.start();
    inMemoryLevelDBAliasMapClient.setConf(conf);

    BlockAliasMap.Reader<FileRegion> reader = inMemoryLevelDBAliasMapClient.getReader(null, BPID);
    Block block = new Block(42, 43, 44);
    byte[] nonce = "blackbird".getBytes();
    ProvidedStorageLocation providedStorageLocation
            = new ProvidedStorageLocation(new Path("cuckoo"),
            45, 46, nonce);
    BlockAliasMap.Writer<FileRegion> writer =
            inMemoryLevelDBAliasMapClient.getWriter(null, BPID);
//    writer.store(new FileRegion(block, providedStorageLocation));
//    writer.remove(block);
    int cnt = 1;
    System.out.println("****************************************");
    for (FileRegion fileRegion : reader) {
      cnt++;
      System.out.println(fileRegion.getProvidedStorageLocation().getPath());
      if (cnt >= 10) {
        break;
      }
    }
  }
}