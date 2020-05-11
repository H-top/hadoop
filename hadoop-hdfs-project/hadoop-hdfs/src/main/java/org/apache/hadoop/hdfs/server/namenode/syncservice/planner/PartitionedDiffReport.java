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
package org.apache.hadoop.hdfs.server.namenode.syncservice.planner;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.server.namenode.syncservice.SyncServiceFileFilter;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.CREATE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.DELETE;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.MODIFY;
import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.RENAME;

/**
 * 将snapshot diffreport分组
 */
public class PartitionedDiffReport {

  private static final Comparator<? super DiffReportEntry>
      reverseSourceNameOrder = (Comparator<DiffReportEntry>)
      (o1, o2) -> {
        String sourcePath1 = DFSUtil.bytes2String(o1.getSourcePath());
        String sourcePath2 = DFSUtil.bytes2String(o2.getSourcePath());
        return sourcePath2.compareTo(sourcePath1);
      };
  private List<RenameEntryWithTemporaryName> renames;
  private List<TranslatedEntry> deletes;
  private List<TranslatedEntry> modifies;
  private List<TranslatedEntry> creates;
  private List<DiffReportEntry> createsFromRenames;

  @VisibleForTesting
  PartitionedDiffReport(List<RenameEntryWithTemporaryName> renames,
      List<TranslatedEntry> deletes, List<TranslatedEntry> modifies,
      List<TranslatedEntry> creates, List<DiffReportEntry> createsFromRenames) {
    this.renames = renames;
    this.deletes = deletes;
    this.modifies = modifies;
    this.creates = creates;
    this.createsFromRenames = createsFromRenames;
  }

  /**
   * 对于rename操作，如果source或者target被filter掉了，那么实际的operation不是rename
   * 可能是create、delete、rename
   */
  public static ResultingOperation determineResultingOperation(
      DiffReportEntry diffReportEntry,
      SyncServiceFileFilter syncServiceFileFilter) {
    boolean isSourceExcluded = syncServiceFileFilter.isExcluded(
        new File(DFSUtil.bytes2String(diffReportEntry.getSourcePath())));
    boolean isTargetExcluded = syncServiceFileFilter.isExcluded(
        new File(DFSUtil.bytes2String(diffReportEntry.getTargetPath())));

    if (isSourceExcluded && isTargetExcluded) {
      return ResultingOperation.NOOP;
    } else if (isSourceExcluded && !isTargetExcluded) {
      return ResultingOperation.CREATE;
    } else if (!isSourceExcluded && isTargetExcluded) {
      return ResultingOperation.DELETE;
    } else {
      return ResultingOperation.RENAME;
    }
  }

  /**
   * 将diffReport中的entry按照实际操作类型分组，并将分组添加到对应的list中
   * rename entry需要额外处理
   */
  public static PartitionedDiffReport partition(SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {
    //将DiffReportEntry中的RENAME根据ResultingOperation分组，将rename操作变为实际的操作
    // TODO triagedMap中的DELETE entry不需要操作??
    //是否需要将该操作sync到s3？？目前是没有？？
    Map<ResultingOperation, List<DiffReportEntry>> triagedMap =
        diffReport
            .getDiffList()
            .stream()
            .filter(diffReportEntry -> diffReportEntry.getType() == RENAME)
            .collect(Collectors.groupingBy(diffReportEntry ->
                determineResultingOperation(diffReportEntry, syncServiceFileFilter)));
    //真正的rename entry
    List<DiffReportEntry> renames = triagedMap.getOrDefault(ResultingOperation.RENAME,
        Collections.emptyList());
    List<RenameEntryWithTemporaryName> renameEntries =
        getRenameEntriesAndGenerateTemporaryNames(renames, diffReport);
    //处理不同类型的entry
    List<TranslatedEntry> translatedDeletes =
        handleDeletes(renameEntries,
            diffReport, syncServiceFileFilter);

    List<TranslatedEntry> translatedModifies =
        handleModifies(renameEntries,
            diffReport, syncServiceFileFilter);

    List<TranslatedEntry> translatedCreates =
        handleCreates(renameEntries,
            diffReport, syncServiceFileFilter);

    List<DiffReportEntry> createsFromRenames =
        triagedMap.getOrDefault(ResultingOperation.CREATE,
            Collections.emptyList());

    return new PartitionedDiffReport(renameEntries, translatedDeletes,
        translatedModifies, translatedCreates, createsFromRenames);
  }

  @VisibleForTesting
  static List<TranslatedEntry> handleDeletes(
      List<RenameEntryWithTemporaryName> renamedToTemps,
      SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {

    return handleEntries(DELETE, PartitionedDiffReport::translateToTemporaryName,
        renamedToTemps, diffReport, syncServiceFileFilter);
  }

  @VisibleForTesting
  static List<TranslatedEntry> handleModifies(
      List<RenameEntryWithTemporaryName> renamedToTemps,
      SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {

    return handleEntries(MODIFY, PartitionedDiffReport::translateToTargetName,
        renamedToTemps, diffReport, syncServiceFileFilter);
  }

  @VisibleForTesting
  static List<TranslatedEntry> handleCreates(
      List<RenameEntryWithTemporaryName> renamedToTemps,
      SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {

    return handleEntries(CREATE, PartitionedDiffReport::translateToTargetName,
        renamedToTemps, diffReport, syncServiceFileFilter);
  }

  /**
   * 将rename entry根据source path排序，并将rename entry和temp name（tmp-UUID）封装后返回
   */
  @VisibleForTesting
  static List<RenameEntryWithTemporaryName>
  getRenameEntriesAndGenerateTemporaryNames(List<DiffReportEntry> renameEntries, SnapshotDiffReport diffReport) {
    return renameEntries
        .stream()
        .sorted(reverseSourceNameOrder)
        .map(entry -> new RenameEntryWithTemporaryName(entry, diffReport))
        .collect(Collectors.toList());
  }

  /**
   * 从SnapshotDiffReport获取DiffType的entry，转换为temp TranslatedEntry后返回
   * diffreport-->entry（entry是否在rename的子目录中，translatedname不同）
   */
  static List<TranslatedEntry> handleEntries(DiffType diffType,
      BiFunction<DiffReportEntry, List<RenameEntryWithTemporaryName>,
          TranslatedEntry> translationFunction,
      List<RenameEntryWithTemporaryName> renamedToTemps,
      SnapshotDiffReport diffReport,
      SyncServiceFileFilter syncServiceFileFilter) {
    //从diffReport过滤出diffType的DiffReportEntry
    List<DiffReportEntry> entries = diffReport.getDiffList().stream()
        .filter(diffReportEntry -> diffReportEntry.getType() == diffType)
        .collect(Collectors.toList());
    //获取转换为temporary name后的entries；将entries转换为TranslatedEntry
    List<TranslatedEntry> translatedEntries = entries
        .stream()
        .flatMap(entry -> {
          TranslatedEntry translatedEntry = translationFunction.apply(entry, renamedToTemps);
          if (syncServiceFileFilter.isExcluded(new File(translatedEntry.getTranslatedName()))) {
            return Stream.empty();
          } else {
            return Stream.of(translatedEntry);
          }
        })
        .collect(Collectors.toList());

    return translatedEntries;
  }

  /**
   * 将entry装换为TranslatedEntry
   * 如果rename dir下的文件有修改，则entry的translatename为：renametmp+renamesource
   * 如果diffentry不是在rename的子目录下，则translatedname为sourcepath
   */
  private static TranslatedEntry translateToTemporaryName(DiffReportEntry entry,
      List<RenameEntryWithTemporaryName> renamesWithRenameEntryWithTemporaryNames) {

    for (RenameEntryWithTemporaryName renameItem :
        renamesWithRenameEntryWithTemporaryNames) {
      byte[] renameSourcePath = renameItem.getEntry().getSourcePath();
      byte[] sourcePath = entry.getSourcePath();
      if (sourcePath.equals(renameSourcePath)) {
        //文件被rename，并且还发生了其他修改
        //if equal, this is two different things

        //remane文件夹里的文件发生了修改
      } else if (isParentOf(renameSourcePath,
          sourcePath)) {

        return TranslatedEntry.withTemporaryName(entry, renameItem);
      }
    }

    //No rename found. Keeping original name
    return TranslatedEntry.withNoRename(entry);

  }

  /**
   * Probe for a path being a parent of another.
   *
   * @param parent
   * @param child
   * @return true if the parent's path matches the start of the child's
   */
  private static boolean isParentOf(byte[] parent, byte[] child) {
    String parentPath = DFSUtil.bytes2String(parent);
    String childPath = DFSUtil.bytes2String(child);
    if (!parentPath.endsWith(Path.SEPARATOR)) {
      parentPath += Path.SEPARATOR;
    }

    return childPath.length() > parentPath.length() &&
        childPath.startsWith(parentPath);
  }

  /**
   * 转换为target name
   * 如果rename dir下的文件有修改，则entry的translatename为：renametarget+renamesource
   * 如果diffentry不是在rename的子目录下，则translatedname为sourcepath
   */
  private static TranslatedEntry translateToTargetName(DiffReportEntry entry,
      List<RenameEntryWithTemporaryName> renamesWithRenameEntryWithTemporaryNames) {

    for (RenameEntryWithTemporaryName renameItem :
        renamesWithRenameEntryWithTemporaryNames) {
      if (entry.getSourcePath().equals(renameItem.getEntry().getSourcePath())) {
        //if equal, this is two different things
      } else if (isParentOf(renameItem.getEntry().getSourcePath(),
          entry.getSourcePath())) {

        return TranslatedEntry.withTargetName(entry, renameItem);
      }
    }

    //No rename found. Keeping original name
    return TranslatedEntry.withNoRename(entry);

  }

  public List<RenameEntryWithTemporaryName> getRenames() {
    return renames;
  }

  public List<TranslatedEntry> getDeletes() {
    return deletes;
  }

  public List<TranslatedEntry> getModifies() {
    return modifies;
  }

  public List<TranslatedEntry> getCreates() {
    return creates;
  }

  public List<DiffReportEntry> getCreatesFromRenames() {
    return createsFromRenames;
  }

  /**
   * enum
   */
  public enum ResultingOperation {
    RENAME, CREATE, DELETE, NOOP
  }

  /**
   * 封装entry和temp name
   */
  public static class RenameEntryWithTemporaryName {

    private DiffReportEntry entry;
    private String temporaryName;

    public RenameEntryWithTemporaryName(DiffReportEntry entry, SnapshotDiffReport diffReport) {
      this.entry = entry;
      this.temporaryName = "tmp-" + diffReport.getFromSnapshot() + "-" + diffReport.getLaterSnapshotName();
    }
    public RenameEntryWithTemporaryName(DiffReportEntry entry) {
      this.entry = entry;
      this.temporaryName = "tmp-" + UUID.randomUUID().toString();
    }

    public DiffReportEntry getEntry() {
      return entry;
    }

    public String getTemporaryName() {
      return temporaryName;
    }
  }

  /**
   * 封装entry和translated name
   */
  public static class TranslatedEntry {
    private DiffReportEntry entry;
    private String translatedName;

    private TranslatedEntry(DiffReportEntry entry, String translatedName) {
      this.entry = entry;
      this.translatedName = translatedName;
    }

    /**
     * entry + entry source name
     */
    public static TranslatedEntry withNoRename(DiffReportEntry entry) {
      return new TranslatedEntry(entry,
          DFSUtil.bytes2String(entry.getSourcePath()));
    }

    /**
     * entry + RenameEntryWithTemporaryName tmpname
     */
    public static TranslatedEntry withTemporaryName(DiffReportEntry entry,
        RenameEntryWithTemporaryName renameItem) {
      String originalName = DFSUtil.bytes2String(entry.getSourcePath());

      String renameEntryName =
          DFSUtil.bytes2String(renameItem.getEntry().getSourcePath());


      //the next line can only work if this assert is true. Doublechecking...
      assert originalName.startsWith(renameEntryName);
      String translatedName = renameItem.getTemporaryName() +
          originalName.substring(renameEntryName.length());

      return new TranslatedEntry(entry, translatedName);
    }

    /**
     * entry + RenameEntryWithTemporaryName target name
     */
    public static TranslatedEntry withTargetName(DiffReportEntry entry,
        RenameEntryWithTemporaryName renameItem) {
      String originalName = DFSUtil.bytes2String(entry.getSourcePath());

      String renameEntryName =
          DFSUtil.bytes2String(renameItem.getEntry().getSourcePath());


      //the next line can only work if this assert is true. Doublechecking...
      assert originalName.startsWith(renameEntryName);
      String translatedName = DFSUtil.bytes2String(renameItem.getEntry()
          .getTargetPath()) + originalName.substring(renameEntryName.length());

      return new TranslatedEntry(entry, translatedName);
    }

    public DiffReportEntry getEntry() {
      return entry;
    }

    public String getTranslatedName() {
      return translatedName;
    }
  }

}
