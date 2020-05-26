/*
 * Copyright (C) 2018 https://github.com/Minamoto54
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 * Modifications copyright (C) 2018 Robert Hjertmann Christiansen
 *
 */
package dk.hjertmann.log4j2.aws.kinesis;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.StringLayout;
import org.apache.logging.log4j.core.appender.ManagerFactory;
import org.apache.logging.log4j.core.appender.WriterManager;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.StreamStatus;
import dk.hjertmann.log4j2.aws.kinesis.BatchKinesisManagerFactory.InitParameters;

public class BatchKinesisManagerFactory implements ManagerFactory<WriterManager, InitParameters> {

  public static class BatchKinesisWriter extends Writer {

    private final InitParameters params;
    private final List<PutRecordsRequestEntry> batchBufferList;
    private final ScheduledExecutorService scheduledExecutor;
    private AmazonKinesisAsync kinesisClient;
    private ScheduledFuture<?> awaitTask;

    public BatchKinesisWriter(final InitParameters params) {
      this.params = params;
      initKinesisClient();
      batchBufferList = new ArrayList<>(params.bufferSize);
      scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
      reScheduleTask();
    }

    @Override
    public void close() throws IOException {
      scheduledExecutor.shutdownNow();
      flushBatchBuffer(false);
      kinesisClient.shutdown();
    }

    @Override
    public void flush() throws IOException {}

    private synchronized void flushBatchBuffer() {
      flushBatchBuffer(true);
    }

    private synchronized void flushBatchBuffer(final boolean async) {
      if (batchBufferList.size() > 0) {
        List<PutRecordsRequestEntry> batchBufferListCopy = new ArrayList<>(batchBufferList);
        batchBufferList.clear();

        LogManager.getLogger().trace("flushBatchBuffer; calling putRecords with size {}",
            batchBufferListCopy.size());
        putRecords(batchBufferListCopy, async);
      } else {
        LogManager.getLogger().trace("flushBatchBuffer; nothing in buffer");
      }
    }

    private void initKinesisClient() {
      ClientConfiguration clientConfiguration = new ClientConfiguration();
      clientConfiguration = AwsUtil.setProxySettingsFromSystemProperties(clientConfiguration);
      clientConfiguration.setMaxErrorRetry(params.maxRetries);
      clientConfiguration
          .setRetryPolicy(new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
              PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, params.maxRetries, true));

      try {
        AmazonKinesisAsyncClientBuilder configuration = AmazonKinesisAsyncClientBuilder.standard()
            .withCredentials(new DefaultAWSCredentialsProviderChain())
            .withClientConfiguration(clientConfiguration);
        if (StringUtils.isBlank(params.endpoint)) {
          configuration.withRegion(params.region);
        } else {
          EndpointConfiguration endpointConfiguration =
              new EndpointConfiguration(params.endpoint, params.region);
          configuration.withEndpointConfiguration(endpointConfiguration);
        }
        kinesisClient = configuration
            .build();

        final DescribeStreamResult describeResult = kinesisClient.describeStream(
            new DescribeStreamRequest().withStreamName(params.streamName));
        final String streamStatus =
            describeResult.getStreamDescription().getStreamStatus();
        if (!StreamStatus.ACTIVE.name().equals(streamStatus)) {
          throw new IllegalStateException("Stream " + params.streamName
              + " is not ready (in active status) for appender: " + params.name);
        }
      } catch (final Exception e) {
        throw new IllegalStateException("Stream " + params.streamName
            + " doesn't exist for appender: " + params.name, e);
      }
    }

    private void putRecords(final List<PutRecordsRequestEntry> batchBufferListCopy,
        final boolean async) {

      LogManager.getLogger().trace("putRecords; called with size {}, async {}",
          batchBufferListCopy.size(), async);

      try {
        if (async) {
          kinesisClient.putRecordsAsync(
              new PutRecordsRequest().withStreamName(params.streamName)
                  .withRecords(batchBufferListCopy));
        } else {
          kinesisClient
              .putRecords(new PutRecordsRequest().withStreamName(params.streamName)
                  .withRecords(batchBufferListCopy));
        }
      } catch (final Exception ex) {
        LogManager.getLogger()
            .error("Failed to schedule log entry for publishing into Kinesis stream: "
                + params.streamName);
      }
    }

    private void reScheduleTask() {
      if (awaitTask != null) {
        awaitTask.cancel(false);
      }
      awaitTask = scheduledExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          flushBatchBuffer();
          reScheduleTask();
        }
      }, params.maxPutRecordDelay, TimeUnit.MINUTES);
    }

    private synchronized void transferData(final PutRecordsRequestEntry logEntry) {
      batchBufferList.add(logEntry);
      LogManager.getLogger().trace("transferData; size after add {}", batchBufferList.size());

      if (batchBufferList.size() >= params.bufferSize) {
        LogManager.getLogger().trace("transferData; do flush buffer");
        flushBatchBuffer();
        reScheduleTask();
      } else {
        LogManager.getLogger().trace("transferData; do not flush buffer");
      }
    }

    @Override
    public void write(final char[] cbuf, final int off, final int len) throws IOException {
      final byte[] data = new String(cbuf, off, len).getBytes(params.charset);
      if (len > 1024000) {
        LogManager.getLogger()
            .error("A log its size larger than 1000 KB can't be sent to Kinesis.\n"
                + new String(cbuf, off, len));
        return;
      }

      PutRecordsRequestEntry logEntry = new PutRecordsRequestEntry().withData(ByteBuffer.wrap(data))
          .withPartitionKey(UUID.randomUUID().toString());
      LogManager.getLogger().trace("write; len {}, logEntry {}", len, logEntry);

      transferData(logEntry);
    }
  }

  public static class InitParameters {
    String name;
    String streamName;
    String endpoint = "";
    Charset charset = StandardCharsets.UTF_8;
    int maxRetries = 3;
    String region;
    int bufferSize = 100; // records, 5 ~ 500.
    int maxPutRecordDelay = 5; // minute, 1 ~ 60.
    StringLayout layout;

    public InitParameters(final String name, final String streamName,
        final String endpoint, final String encoding,
        final int maxRetries, final String region, final int bufferSize,
        final int maxPutRecordDelay,
        final StringLayout layout) {

      this.name = name;
      if (StringUtils.isBlank(streamName)) {
        throw new IllegalArgumentException(
            "Invalid configuration - streamName cannot be null for appender: " + name);
      }
      this.streamName = streamName.trim();

      if (StringUtils.isNotBlank(encoding)) {
        try {
          charset = Charset.forName(encoding.trim());
        } catch (final Exception e) {}
      }

      if (StringUtils.isNotBlank(endpoint)) {
        this.endpoint = endpoint.trim();
      }

      this.maxRetries = getDefaultIfZero(maxRetries, this.maxRetries);

      if (StringUtils.isBlank(region)) {
        throw new IllegalArgumentException(
            "Invalid configuration - region cannot be null for appender: " + name);
      }
      this.region = region.trim();
      this.bufferSize = Math.min(Math.max(5, getDefaultIfZero(bufferSize, this.bufferSize)), 500);
      this.maxPutRecordDelay =
          Math.min(Math.max(1, getDefaultIfZero(maxPutRecordDelay, this.maxPutRecordDelay)), 60);
      this.layout = layout;
    }

    private int getDefaultIfZero(final int test, final int def) {
      return test == 0 ? def : test;
    }
  }

  @Override
  public WriterManager createManager(final String name, final InitParameters params) {
    return new WriterManager(new BatchKinesisWriter(params), params.streamName, params.layout,
        false);
  }

}
