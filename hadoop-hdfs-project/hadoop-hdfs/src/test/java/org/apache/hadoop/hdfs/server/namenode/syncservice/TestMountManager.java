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
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithSnapshotFeature;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.syncservice.MiniDFSClusterRule;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedSyncMountSnapshotUpdateFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
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

import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.CREATE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.MODIFY;
import static org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PartitionedDiffReport.partition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMountManager {

  @Rule
  public MiniDFSClusterRule clusterRule = new MiniDFSClusterRule(false);

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
    MiniDFSCluster cluster = clusterRule.getCluster();
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    Path localBackupPath = new Path("/test");
    fileSystem.mkdirs(localBackupPath, FsPermission.getDefault());
    fileSystem.mkdirs(new Path("/test/sub1"));
    FSDataOutputStream out = null;
    out = fileSystem.createFile(new Path("/test/worker1")).build();
    out.write("worker1".getBytes());
    out.close();
    Configuration conf = new Configuration();
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem namesystem = nameNode.getNamesystem();
    BlockManager blockManager = namesystem.getBlockManager();
    INode inode_root = namesystem.getFSDirectory().getINode(localBackupPath.toString());
    namesystem.allowSnapshot(localBackupPath.toString());
    String snapshotName = "s0";
    namesystem.createSnapshot(localBackupPath.toString(), snapshotName, false);

    fileSystem.mkdirs(new Path("/test/sub2"));
    INodeDirectory iNodeDirectory = inode_root.asDirectory();
    DirectoryWithSnapshotFeature.DirectoryDiffList diffs = iNodeDirectory.getDirectorySnapshottableFeature().getDiffs();
    System.out.println("****************create dir******************");
    System.out.println(diffs);

    out = fileSystem.createFile(new Path("/test/worker2")).build();
    out.write("worker2".getBytes());
    out.close();
    out = fileSystem.createFile(new Path("/test/worker3")).build();
    out.write("worker3".getBytes());
    out.close();
    diffs = iNodeDirectory.getDirectorySnapshottableFeature().getDiffs();
    System.out.println("****************create file******************");
    System.out.println(diffs);
    out = fileSystem.appendFile(new Path("/test/worker1")).build();
    out.write("add".getBytes());
    out.close();
    fileSystem.delete(new Path("/test/worker2"), true);
    fileSystem.rename(new Path("/test/worker3"), new Path("/test/worker3-renamed"));
    fileSystem.rename(new Path("/test/sub1"), new Path("/test/sub11"));
    out = fileSystem.createFile(new Path("/test/sub11/worker")).build();
    out.write("worker".getBytes());
    out.close();
    diffs = iNodeDirectory.getDirectorySnapshottableFeature().getDiffs();
    System.out.println("****************M******************");
    System.out.println(diffs);
    namesystem.createSnapshot(localBackupPath.toString(), "s1", false);
    SnapshotDiffReport diffReport = fileSystem.getSnapshotDiffReport(localBackupPath, snapshotName, "s1");
    System.out.println("*************************************");
    System.out.println(diffReport);
    List<DiffReportEntry> entries = diffReport.getDiffList();
    System.out.println(entries.size());
    for (DiffReportEntry entry : entries) {
      System.out.println(entry);
    }
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

  private int getTargetSnapshotId(SnapshotDiffReport diffReport)
          throws UnresolvedLinkException, AccessControlException,
          ParentNotDirectoryException {
    MiniDFSCluster cluster = clusterRule.getCluster();
    NameNode nameNode = cluster.getNameNode();
    FSNamesystem namesystem = nameNode.getNamesystem();
    INode localBackupPathINode = namesystem.getFSDirectory()
            .getINode(diffReport.getSnapshotRoot());
    INodeDirectory localBackupPathINodeDirectory =
            localBackupPathINode.asDirectory();
    Snapshot toSnapshot = localBackupPathINodeDirectory.getSnapshot(
            diffReport.getLaterSnapshotName().getBytes());
    return toSnapshot.getId();
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
}