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
package org.apache.hadoop.fs;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;

import static org.apache.hadoop.fs.Path.mergePaths;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * A MultipartUploader that uses the basic FileSystem commands.
 * This is done in three stages:
 * <ul>
 *   <li>Init - create a temp {@code _multipart} directory.</li>
 *   <li>PutPart - copying the individual parts of the file to the temp
 *   directory.</li>
 *   <li>Complete - use {@link FileSystem#concat} to merge the files;
 *   and then delete the temp directory.</li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FileSystemMultipartUploader extends MultipartUploader {

  private final FileSystem fs;
  public static final String HEADER = "FileSystem-part01";

  public FileSystemMultipartUploader(FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public UploadHandle initialize(Path filePath) throws IOException {
    Path collectorPath = createCollectorPath(filePath);
    fs.mkdirs(collectorPath, FsPermission.getDirDefault());

    ByteBuffer byteBuffer = ByteBuffer.wrap(
        collectorPath.toString().getBytes(Charsets.UTF_8));
    return BBUploadHandle.from(byteBuffer);
  }

  @Override
  public PartHandle putPart(Path filePath, InputStream inputStream,
      int partNumber, UploadHandle uploadId, long lengthInBytes)
      throws IOException {
    checkPutArguments(filePath, inputStream, partNumber, uploadId,
        lengthInBytes);
    byte[] uploadIdByteArray = uploadId.toByteArray();
    checkUploadId(uploadIdByteArray);
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));
    Path partPath =
        mergePaths(collectorPath, mergePaths(new Path(Path.SEPARATOR),
            new Path(Integer.toString(partNumber) + ".part")));
    try(FSDataOutputStream fsDataOutputStream =
            fs.createFile(partPath).build()) {
      IOUtils.copy(inputStream, fsDataOutputStream, 4096);
    } finally {
      cleanupWithLogger(LOG, inputStream);
    }
    return BBPartHandle.from(ByteBuffer.wrap(
        buildPartHandlePayload(partNumber, partPath.toString())));
  }

  private Path createCollectorPath(Path filePath) {
    String uuid = UUID.randomUUID().toString();
    return mergePaths(filePath.getParent(),
        mergePaths(new Path(filePath.getName().split("\\.")[0]),
            mergePaths(new Path("_multipart_" + uuid),
                new Path(Path.SEPARATOR))));
  }

  private PathHandle getPathHandle(Path filePath) throws IOException {
    FileStatus status = fs.getFileStatus(filePath);
    return fs.getPathHandle(status);
  }

  private long totalPartsLen(List<Path> partHandles) throws IOException {
    long totalLen = 0;
    for (Path p: partHandles) {
      totalLen += fs.getFileStatus(p).getLen();
    }
    return totalLen;
  }

  @Override
  @SuppressWarnings("deprecation") // rename w/ OVERWRITE
  public PathHandle complete(Path filePath, List<PartHandle> handleList,
      UploadHandle multipartUploadId) throws IOException {

    checkUploadId(multipartUploadId.toByteArray());
    Map<Integer, Path> pathMap = Maps.newHashMap();
    for (PartHandle handle : handleList) {
      byte[] payload = handle.toByteArray();
      Pair<Integer, Path> result = parsePartHandlePayload(payload);
      pathMap.put(result.getLeft(), result.getRight());
    }
    List<Map.Entry<Integer, Path>> handles =
        new ArrayList<>(pathMap.entrySet());
    handles.sort(Comparator.comparingInt(Map.Entry::getKey));

    List<Path> partHandles = handles
        .stream()
        .map(pair -> {
          byte[] byteArray = pair.getValue().toString().getBytes();
          return new Path(new String(byteArray, 0, byteArray.length,
              Charsets.UTF_8));
        })
        .collect(Collectors.toList());

    byte[] uploadIdByteArray = multipartUploadId.toByteArray();
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));

    boolean emptyFile = totalPartsLen(partHandles) == 0;
    if (emptyFile) {
      fs.create(filePath).close();
    } else {
      Path filePathInsideCollector = mergePaths(collectorPath,
          new Path(Path.SEPARATOR + filePath.getName()));
      fs.create(filePathInsideCollector).close();
      fs.concat(filePathInsideCollector,
          partHandles.toArray(new Path[handles.size()]));
      fs.rename(filePathInsideCollector, filePath, Options.Rename.OVERWRITE);
    }
    fs.delete(collectorPath, true);
    return getPathHandle(filePath);
  }

  @Override
  public void abort(Path filePath, UploadHandle uploadId) throws IOException {
    byte[] uploadIdByteArray = uploadId.toByteArray();
    checkUploadId(uploadIdByteArray);
    Path collectorPath = new Path(new String(uploadIdByteArray, 0,
        uploadIdByteArray.length, Charsets.UTF_8));

    // force a check for a file existing; raises FNFE if not found
    fs.getFileStatus(collectorPath);
    fs.delete(collectorPath, true);
  }

  /**
   * Factory for creating MultipartUploaderFactory objects for file://
   * filesystems.
   */
  public static class Factory extends MultipartUploaderFactory {
    protected MultipartUploader createMultipartUploader(FileSystem fs,
        Configuration conf) {
      if (fs.getScheme().equals("file")) {
        return new FileSystemMultipartUploader(fs);
      }
      return null;
    }
  }

  @VisibleForTesting
  static byte[] buildPartHandlePayload(int partNumber, String partPath)
          throws IOException {
    Preconditions.checkArgument(partNumber > 0,
            "Invalid partNumber");
    Preconditions.checkArgument(StringUtils.isNotEmpty(partPath),
            "Invalid partPath");

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try(DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeUTF(HEADER);
      output.writeInt(partNumber);
      output.writeUTF(partPath);
    }
    return bytes.toByteArray();
  }

  @VisibleForTesting
  static Pair<Integer, Path> parsePartHandlePayload(byte[] data)
          throws IOException {

    try(DataInputStream input =
                new DataInputStream(new ByteArrayInputStream(data))) {
      final String header = input.readUTF();
      if (!HEADER.equals(header)) {
        throw new IOException("Wrong header string: \"" + header + "\"");
      }
      final int partNumber = input.readInt();
      if (partNumber < 0) {
        throw new IOException("Negative part number");
      }
      final String partPath = input.readUTF();
      return Pair.of(partNumber, new Path(partPath));
    }
  }
}
