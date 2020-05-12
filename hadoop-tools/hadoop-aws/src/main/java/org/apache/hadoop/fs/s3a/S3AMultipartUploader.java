/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.s3a;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BBPartHandle;
import org.apache.hadoop.fs.BBUploadHandle;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.MultipartUploader;
import org.apache.hadoop.fs.MultipartUploaderFactory;
import org.apache.hadoop.fs.PartHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.UploadHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A;

/**
 * MultipartUploader for S3AFileSystem. This uses the S3 multipart
 * upload mechanism.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class S3AMultipartUploader extends MultipartUploader {

  private final S3AFileSystem s3a;

  /** Header for Parts: {@value}. */

  public static final String HEADER = "S3A-part01";

  public S3AMultipartUploader(FileSystem fs, Configuration conf) {
    Preconditions.checkArgument(fs instanceof S3AFileSystem,
        "Wrong filesystem: expected S3A but got %s", fs);
    s3a = (S3AFileSystem) fs;
  }

  @Override
  public UploadHandle initialize(Path filePath) throws IOException {
    final WriteOperationHelper writeHelper = s3a.getWriteOperationHelper();
    String key = s3a.pathToKey(filePath);
    String uploadId = writeHelper.initiateMultiPartUpload(key);
    return BBUploadHandle.from(ByteBuffer.wrap(
        uploadId.getBytes(Charsets.UTF_8)));
  }

  @Override
  public PartHandle putPart(Path filePath, InputStream inputStream,
      int partNumber, UploadHandle uploadId, long lengthInBytes)
      throws IOException {
    checkPutArguments(filePath, inputStream, partNumber, uploadId,
        lengthInBytes);
    byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);
    String key = s3a.pathToKey(filePath);
    final WriteOperationHelper writeHelper = s3a.getWriteOperationHelper();
    String uploadIdString = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    UploadPartRequest request = writeHelper.newUploadPartRequest(key,
        uploadIdString, partNumber, (int) lengthInBytes, inputStream, null, 0L);
    UploadPartResult result = writeHelper.uploadPart(request);
    String eTag = result.getETag();
    LOG.info("eTag for part {}: {}", partNumber, eTag);
    return BBPartHandle.from(
        ByteBuffer.wrap(
            buildPartHandlePayload(eTag, partNumber, lengthInBytes)));
  }

  @Override
  public PathHandle complete(Path filePath,
      List<PartHandle> handleList,
      UploadHandle uploadId)
      throws IOException {
    byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);
    final WriteOperationHelper writeHelper = s3a.getWriteOperationHelper();
    String key = s3a.pathToKey(filePath);

    String uploadIdStr = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    ArrayList<PartETag> eTags = new ArrayList<>();
    eTags.ensureCapacity(handleList.size());
    long totalLength = 0;
    for (PartHandle handle : handleList) {
      byte[] payload = handle.toByteArray();
      Pair<Long, Pair<Integer, String>> result = parsePartHandlePayload(payload);
      int partNumber = result.getRight().getLeft();
      Preconditions.checkArgument(partNumber > 0, "Invalid part handle index %s", partNumber);
      totalLength += result.getLeft();
      eTags.add(new PartETag(partNumber, result.getRight().getRight()));
    }
    AtomicInteger errorCount = new AtomicInteger(0);
    CompleteMultipartUploadResult result = writeHelper.completeMPUwithRetries(
        key, uploadIdStr, eTags, totalLength, errorCount);

    byte[] eTag = result.getETag().getBytes(Charsets.UTF_8);
    return (PathHandle) () -> ByteBuffer.wrap(eTag);
  }

  @Override
  public void abort(Path filePath, UploadHandle uploadId) throws IOException {
    final byte[] uploadIdBytes = uploadId.toByteArray();
    checkUploadId(uploadIdBytes);
    final WriteOperationHelper writeHelper = s3a.getWriteOperationHelper();
    String key = s3a.pathToKey(filePath);
    String uploadIdString = new String(uploadIdBytes, 0, uploadIdBytes.length,
        Charsets.UTF_8);
    writeHelper.abortMultipartCommit(key, uploadIdString);
  }

  /**
   * Factory for creating MultipartUploader objects for s3a:// FileSystems.
   */
  public static class Factory extends MultipartUploaderFactory {
    @Override
    protected MultipartUploader createMultipartUploader(FileSystem fs,
        Configuration conf) {
      if (FS_S3A.equals(fs.getScheme())) {
        return new S3AMultipartUploader(fs, conf);
      }
      return null;
    }
  }

  /**
   * Build the payload for marshalling.
   * @param eTag upload etag
   * @param len length
   * @return a byte array to marshall.
   * @throws IOException error writing the payload
   */
  @VisibleForTesting
  static byte[] buildPartHandlePayload(String eTag, int partNumber, long len)
      throws IOException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(eTag),
        "Empty etag");
    Preconditions.checkArgument(partNumber > 0,
            "Invalid partNumber");
    Preconditions.checkArgument(len >= 0,
        "Invalid length");

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try(DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeUTF(HEADER);
      output.writeInt(partNumber);
      output.writeLong(len);
      output.writeUTF(eTag);
    }
    return bytes.toByteArray();
  }

  /**
   * Parse the payload marshalled as a part handle.
   * @param data handle data
   * @return the partnumber, length and etag
   * @throws IOException error reading the payload
   */
  @VisibleForTesting
  static Pair<Long, Pair<Integer, String>> parsePartHandlePayload(byte[] data)
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
      final long len = input.readLong();
      final String etag = input.readUTF();
      if (len < 0) {
        throw new IOException("Negative length");
      }
      return Pair.of(len, Pair.of(partNumber, etag));
    }
  }

}
