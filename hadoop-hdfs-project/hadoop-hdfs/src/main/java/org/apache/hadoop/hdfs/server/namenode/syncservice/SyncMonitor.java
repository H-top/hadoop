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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SyncMount;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedPlan;
import org.apache.hadoop.hdfs.server.namenode.syncservice.planner.PhasedSyncMountSnapshotUpdateFactory;
import org.apache.hadoop.hdfs.server.namenode.syncservice.scheduler.SyncTaskScheduler;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SchedulableSyncPhase;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SyncMountSnapshotUpdateTracker;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.SyncMountSnapshotUpdateTrackerFactory;
import org.apache.hadoop.hdfs.server.protocol.SyncTaskExecutionResult;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * 维护syncmount update tracker（一个syncmount对应一个tracker？）
 */
public class SyncMonitor {

  public static final Logger LOG = LoggerFactory.getLogger(SyncMonitor.class);
  private final Namesystem namesystem;
  //syncmountid-->syncmount update tracker
  private Map<String, SyncMountSnapshotUpdateTracker> inProgress;
  //维护failed tracker
  private Map<String, SyncMountSnapshotUpdateTracker> trackersFailed;
  private PhasedSyncMountSnapshotUpdateFactory syncMountSnapshotUpdatePlanFactory;
  private SyncTaskScheduler syncTaskScheduler;
  private BlockAliasMap.Writer<FileRegion> aliasMapWriter;
  private Configuration conf;

  public SyncMonitor(Namesystem namesystem, PhasedSyncMountSnapshotUpdateFactory
      syncMountSnapshotUpdatePlanFactory, SyncTaskScheduler syncTaskScheduler,
      BlockAliasMap.Writer<FileRegion> aliasMapWriter, Configuration conf) {
    this.syncMountSnapshotUpdatePlanFactory = syncMountSnapshotUpdatePlanFactory;
    this.syncTaskScheduler = syncTaskScheduler;
    this.aliasMapWriter = aliasMapWriter;
    this.conf = conf;
    this.inProgress = Maps.newConcurrentMap();
    this.trackersFailed = Maps.newConcurrentMap();
    this.namesystem = namesystem;
  }

  /**
   * 调用syncmount对应的SyncMountSnapshotUpdateTracker的markfailed方法标记synctask failed，
   * if tracker is not still valid, 将tracker从inprogress放到trackersfailed，return false
   */
  public boolean markSyncTaskFailed(UUID syncTaskId,
      String syncMountId,
      SyncTaskExecutionResult result) {

    Optional<SyncMountSnapshotUpdateTracker> syncMountSnapshotUpdateTrackerOpt =
        fetchUpdateTracker(syncMountId);

    return syncMountSnapshotUpdateTrackerOpt.map(syncMountSnapshotUpdateTracker -> {
      boolean isTrackerStillValid = syncMountSnapshotUpdateTracker.markFailed(syncTaskId, result);
      if (isTrackerStillValid) {
        return true;
      } else {
        inProgress.remove(syncMountId);
        this.trackersFailed.put(syncMountId, syncMountSnapshotUpdateTracker);
        LOG.error("Issue when remounting sync mount after failed tracker. Tracker removed from trackers in progress.");
        return false;
      }
    })
        .orElse(false);
  }

  /**
   * 调用syncmount对应的SyncMountSnapshotUpdateTracker的markfinished方法标记synctask finished，
   * if tracker is finished, 从inprogress中移除
   */
  public void markSyncTaskFinished(UUID syncTaskId, String syncMountId,
      SyncTaskExecutionResult result) {

    Optional<SyncMountSnapshotUpdateTracker> syncMountSnapshotUpdateTrackerOpt = fetchUpdateTracker(syncMountId);

    syncMountSnapshotUpdateTrackerOpt.ifPresent(syncMountSnapshotUpdateTracker -> {
      syncMountSnapshotUpdateTracker.markFinished(syncTaskId, result);

      if (syncMountSnapshotUpdateTracker.isFinished()) {
        inProgress.remove(syncMountId);
        //tracker finish表示此次snapshot的sync结束，等待timeout进行下一次sync schedule，
        // 所以wait的timeout为两次snapshot sync的最大时间间隔，这里也不需要notify syncmonitor
        //No notification, as this is a timeout thing in the SyncServiceSatisfier
      } else {
        //每次synctask成功都notify syncmonitor进行下一次sync schedule
        //对于failed synctask会重新加入todo list，如果不notify syncmonitor schedulenextwork，那么这些synctask不会被重新schedule，
        //保证failed synctask能被及时schedule，而不是等到syncmonitor timeout
        this.notify();
      }
    });

  }

  @VisibleForTesting
  boolean hasTrackersInProgress() {
    return !inProgress.isEmpty();
  }

  /**
   * 从inProgress获取syncmountid对应的SyncMountSnapshotUpdateTracker
   */
  private Optional<SyncMountSnapshotUpdateTracker> fetchUpdateTracker(String syncMountId) {
    SyncMountSnapshotUpdateTracker syncMountSnapshotUpdateTracker =
        inProgress.get(syncMountId);

    if (syncMountSnapshotUpdateTracker == null) {
      // This can happen when the tracker failed, was removed, and the
      // full resync was not scheduled
      // @ehiggs not sure whether this is really necessary, rediscuss at review time.
      return Optional.empty();
    }
    return Optional.of(syncMountSnapshotUpdateTracker);
  }

  /**
   * 遍历所有的sncmounts并schedule next work
   * 如果inprogress中有对应的记录，则在相应的tracker上schedule；如果没有就新建一个tracker
   */
  void scheduleNextWork() {

    MountManager mountManager = namesystem.getMountManagerSync();
    List<SyncMount> syncMounts = mountManager.getSyncMounts();

    for (SyncMount syncMount : syncMounts) {
      if (namesystem.isInSafeMode()) {
        LOG.debug("Skipping synchronization of SyncMounts as the " +
            "namesystem is in safe mode");
        break;
      }
      if (syncMount.isPaused()) {
        LOG.info("Sync mount is paused");
        continue;
      }

      if (inProgress.containsKey(syncMount.getName())) {
        scheduleNextWorkOnTracker(inProgress.get(syncMount.getName()), syncMount);
      } else {
        scheduleNewSyncMountSnapshotUpdate(syncMount);
      }

    }
  }

  /**
   * 对syncmount进行full resync，创建新的tracker
   * no need for full resync, just resync task between fromSnapshot and toSnapshot
   */
  public void fullResync(String syncMountId, BlockAliasMap.Reader<FileRegion> aliasMapReader) throws IOException {
    MountManager mountManager = namesystem.getMountManagerSync();
    SyncMount syncMount = mountManager.getSyncMount(syncMountId);
    //TODO get diffreport between previous fromSnapshot and toSnapshot
    SnapshotDiffReport diffReport =
        mountManager.forceInitialSnapshot(syncMount.getLocalPath());
    int targetSnapshotId = getTargetSnapshotId(diffReport);
    Optional<Integer> sourceSnapshotId = Optional.empty();
    PhasedPlan planFromDiffReport = syncMountSnapshotUpdatePlanFactory.
        createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
            targetSnapshotId);
    for (FileRegion fileRegion : aliasMapReader) {
      //TODO add nonce SyncMountSnapshotUpdateTrackerImpl#finalizeRenameFileTask
      Path pathInAliasMap = fileRegion.getProvidedStorageLocation().getPath();
      planFromDiffReport.filter(pathInAliasMap);
    }
    SyncMountSnapshotUpdateTracker tracker =
        SyncMountSnapshotUpdateTrackerFactory.create(planFromDiffReport,
            aliasMapWriter, conf);
    scheduleNextWorkOnTracker(tracker, syncMount);
  }

  public boolean blockingCancelTracker(String syncMountId) {
    //Do not remove. Let the mark fail/mark success do this through the normal process
    SyncMountSnapshotUpdateTracker syncMountSnapshotUpdateTracker = inProgress.get(syncMountId);
    if (syncMountSnapshotUpdateTracker == null) {
      //Possible that the tracker already finished by the time the request to
      //cancel comes in.
      return true;
    }
    return syncMountSnapshotUpdateTracker.blockingCancel();
  }

  /**
   * 代表syncmount上有新schedule的work，新建一个tracker，并在该tracker上schedule work
   */
  private void scheduleNewSyncMountSnapshotUpdate(SyncMount syncMount) {

    LOG.info("Planning new SyncMount {}", syncMount);

    if (inProgress.containsKey(syncMount.getName())) {
      LOG.info("SyncMount {} still has unfinished scheduled work, not adding " +
          "additional work", syncMount);
    } else {

      MountManager mountManager = namesystem.getMountManagerSync();

      SnapshotDiffReport diffReport;
      Optional<Integer> sourceSnapshotId;
      int targetSnapshotId;
      try {
        diffReport = mountManager.makeSnapshotAndPerformDiff(
            syncMount.getLocalPath());
        sourceSnapshotId = getSourceSnapshotId(diffReport);
        targetSnapshotId = getTargetSnapshotId(diffReport);
      } catch (IOException e) {
        LOG.error("Failed to take snapshot for: {}", syncMount, e);
        return;
      }

      PhasedPlan planFromDiffReport = syncMountSnapshotUpdatePlanFactory.
          createPlanFromDiffReport(syncMount, diffReport, sourceSnapshotId,
              targetSnapshotId);

      if (planFromDiffReport.isEmpty()) {
        /**
         * The tracker for an empty plan will never finish as there will
         * be no tasks to trigger the finish marking.
         */
        LOG.info("Empty plan, not starting a tracker");
      } else {
        SyncMountSnapshotUpdateTracker tracker =
            SyncMountSnapshotUpdateTrackerFactory.create(planFromDiffReport,
                aliasMapWriter, conf);
        scheduleNextWorkOnTracker(tracker, syncMount);
      }
    }
  }

  /**
   * 将tracker加入inprogress，然后获取tracker的SchedulableSyncPhase，并通过syncTaskScheduler进行schedule
   */
  private void scheduleNextWorkOnTracker(SyncMountSnapshotUpdateTracker tracker, SyncMount syncMount) {
    inProgress.put(syncMount.getName(), tracker);
    SchedulableSyncPhase schedulableSyncPhase = tracker.getNextSchedulablePhase();
    syncTaskScheduler.schedule(schedulableSyncPhase);
  }

  /**
   * 从diffreport获取source snapshot id
   */
  private Optional<Integer> getSourceSnapshotId(SnapshotDiffReport diffReport)
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
  private int getTargetSnapshotId(SnapshotDiffReport diffReport)
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


  public Set<String> getTrackersInProgress() {
    return this.inProgress.keySet();
  }

  public Set<String> getTrackersFailed() {
    return this.trackersFailed.keySet();
  }

}
