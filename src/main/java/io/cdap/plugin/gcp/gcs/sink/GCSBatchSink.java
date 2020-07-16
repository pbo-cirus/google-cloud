/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.sink;

import com.google.auth.Credentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.format.FileFormat;
import io.cdap.plugin.format.plugin.AbstractFileSink;
import io.cdap.plugin.format.plugin.FileSinkProperties;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Writes data to files on Google Cloud Storage.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GCS")
@Description("Writes records to one or more files in a directory on Google Cloud Storage.")
public class GCSBatchSink extends AbstractFileSink<GCSBatchSink.GCSBatchSinkConfig> {
  private final GCSBatchSinkConfig config;

  public GCSBatchSink(GCSBatchSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    super.prepareRun(context);
    String cmekKey = context.getArguments().get(GCPUtils.CMEK_KEY);
    Credentials credentials = config.getServiceAccountFilePath() == null ?
                                null : GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath());
    Storage storage = GCPUtils.getStorage(config.getProject(), credentials);
    Bucket bucket;
    try {
      bucket = storage.get(config.getBucket());
    } catch (StorageException e) {
      RuntimeException re = new RuntimeException(
        String.format("Unable to access or create bucket at path %s. ", config.getBucket())
          + "Ensure you entered the correct bucket path.");
      re.initCause(e);
      throw re;
    }
    if (bucket == null) {
      GCPUtils.createBucket(storage, config.getBucket(), config.getLocation(), cmekKey);
    }
  }

  @Override
  protected Map<String, String> getFileSystemProperties(BatchSinkContext context) {
    return GCPUtils.getFileSystemProperties(config, config.getPath(), new HashMap<>());
  }

  @Override
  protected void recordLineage(LineageRecorder lineageRecorder, List<String> outputFields) {
    lineageRecorder.recordWrite("Write", "Wrote to Google Cloud Storage.", outputFields);
  }

  /**
   * Sink configuration.
   */
  @SuppressWarnings("unused")
  public static class GCSBatchSinkConfig extends GCPReferenceSinkConfig implements FileSinkProperties {
    private static final String NAME_PATH = "path";
    private static final String NAME_SUFFIX = "suffix";
    private static final String NAME_FORMAT = "format";
    private static final String NAME_SCHEMA = "schema";
    private static final String NAME_LOCATION = "location";

    private static final String SCHEME = "gs://";
    @Name(NAME_PATH)
    @Description("The path to write to. For example, gs://<bucket>/path/to/directory")
    @Macro
    private String path;

    @Description("The time format for the output directory that will be appended to the path. " +
      "For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
      "If not specified, nothing will be appended to the path.")
    @Nullable
    @Macro
    private String suffix;

    @Macro
    @Description("The format to write in. The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', "
      + "or 'delimited'.")
    protected String format;

    @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
      + "is anything other than 'delimited'.")
    @Macro
    @Nullable
    private String delimiter;

    @Description("The schema of the data to write. The 'avro' and 'parquet' formats require a schema but other "
      + "formats do not.")
    @Macro
    @Nullable
    private String schema;

    @Name(NAME_LOCATION)
    @Macro
    @Nullable
    @Description("The location where the gcs bucket will get created. " +
                   "This value is ignored if the bucket already exists")
    protected String location;

    @Override
    public void validate() {
      // no-op
    }

    @Override
    public void validate(FailureCollector collector) {
      super.validate(collector);
      // validate that path is valid
      if (!containsMacro(NAME_PATH)) {
        try {
          GCSPath.from(path);
          // Check if bucket exists and is accessible
          Credentials credentials = getServiceAccountFilePath() == null ?
            null : GCPUtils.loadServiceAccountCredentials(getServiceAccountFilePath());
          Storage storage = GCPUtils.getStorage(getProject(), credentials);
          Bucket bucket = storage.get(getBucket());
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_PATH).withStacktrace(e.getStackTrace());
        } catch (IOException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH)
            .withStacktrace(e.getStackTrace());
        } catch (StorageException e) {
          collector.addFailure(e.getMessage(), "Ensure you entered the correct bucket path.")
            .withConfigProperty(NAME_PATH)
            .withStacktrace(e.getStackTrace());
        }
      }
      if (suffix != null && !containsMacro(NAME_SUFFIX)) {
        try {
          new SimpleDateFormat(suffix);
        } catch (IllegalArgumentException e) {
          collector.addFailure("Invalid suffix : " + e.getMessage(), null)
            .withConfigProperty(NAME_SUFFIX).withStacktrace(e.getStackTrace());
        }
      }

      if (!containsMacro(NAME_FORMAT)) {
        try {
          getFormat();
        } catch (IllegalArgumentException e) {
          collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_FORMAT)
            .withStacktrace(e.getStackTrace());
        }
      }

      try {
        getSchema();
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(NAME_SCHEMA).withStacktrace(e.getStackTrace());
      }
    }

    public String getBucket() {
      return GCSPath.from(path).getBucket();
    }

    @Override
    public String getPath() {
      GCSPath gcsPath = GCSPath.from(path);
      return SCHEME + gcsPath.getBucket() + gcsPath.getUri().getPath();
    }

    @Override
    public FileFormat getFormat() {
      return FileFormat.from(format, FileFormat::canWrite);
    }

    @Nullable
    public Schema getSchema() {
      if (containsMacro("schema") || Strings.isNullOrEmpty(schema)) {
        return null;
      }
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage(), e);
      }
    }

    @Nullable
    @Override
    public String getSuffix() {
      return suffix;
    }

    @Nullable
    public String getDelimiter() {
      return delimiter;
    }

    @Nullable
    public String getLocation() {
      return location;
    }
  }
}
