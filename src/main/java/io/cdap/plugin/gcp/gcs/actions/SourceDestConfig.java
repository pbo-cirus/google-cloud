/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.gcs.actions;

import com.google.auth.Credentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Contains common properties for copy/move.
 */
public class SourceDestConfig extends GCPConfig {
  public static final String NAME_SOURCE_PATH = "sourcePath";
  public static final String NAME_DEST_PATH = "destPath";

  @Name(NAME_SOURCE_PATH)
  @Macro
  @Description("Path to a source object or directory.")
  private String sourcePath;

  @Name(NAME_DEST_PATH)
  @Macro
  @Description("Path to the destination. The bucket must already exist.")
  private String destPath;

  @Macro
  @Nullable
  @Description("Whether to overwrite existing objects.")
  private Boolean overwrite;

  public SourceDestConfig() {
    overwrite = false;
  }

  GCSPath getSourcePath() {
    return GCSPath.from(sourcePath);
  }

  GCSPath getDestPath() {
    return GCSPath.from(destPath);
  }

  @Nullable
  Boolean shouldOverwrite() {
    return overwrite;
  }

  public void validate(FailureCollector collector) {
    if (!containsMacro("sourcePath")) {
      try {
        GCSPath gcsSourcePath = getSourcePath();
        // Check if bucket exists and is accessible
        Credentials credentials = getServiceAccountFilePath() == null ?
          null : GCPUtils.loadServiceAccountCredentials(getServiceAccountFilePath());
        Storage storage = GCPUtils.getStorage(getProject(), credentials);
        Bucket bucket = storage.get(gcsSourcePath.getBucket());
        if (bucket == null) {
          collector.addFailure("Bucket does not exist.",
                               "Ensure you entered the correct bucket path.")
            .withConfigProperty(NAME_SOURCE_PATH);
        }
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SOURCE_PATH);
      } catch (IOException e) {
        collector.addFailure(e.getMessage(), "Ensure you entered the correct file path.")
          .withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH)
          .withStacktrace(e.getStackTrace());
      } catch (StorageException e) {
        collector.addFailure(e.getMessage(), "Ensure you entered the correct bucket path.")
          .withConfigProperty(NAME_SOURCE_PATH)
          .withStacktrace(e.getStackTrace());
      }
    }
    if (!containsMacro("destPath")) {
      try {
        GCSPath gcsDestPath = getDestPath();
        // Check if bucket exists and is accessible
        Credentials credentials = getServiceAccountFilePath() == null ?
          null : GCPUtils.loadServiceAccountCredentials(getServiceAccountFilePath());
        Storage storage = GCPUtils.getStorage(getProject(), credentials);
        Bucket bucket = storage.get(gcsDestPath.getBucket());
        if (bucket == null) {
          collector.addFailure("Bucket does not exist.",
                               "Please create the bucket or ensure you entered the correct bucket path.")
            .withConfigProperty(NAME_DEST_PATH);
        }
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_DEST_PATH);
      } catch (IOException e) {
        collector.addFailure(e.getMessage(), "Ensure you entered the correct file path.")
          .withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH)
          .withStacktrace(e.getStackTrace());
      } catch (StorageException e) {
        collector.addFailure(e.getMessage(),
                             "Please create the bucket or ensure you entered the correct bucket path.")
          .withConfigProperty(NAME_DEST_PATH)
          .withStacktrace(e.getStackTrace());
      }
    }
    collector.getOrThrowException();
  }
}
